import axios from "axios";
import { config } from "../configs/Config";
import { logger } from "@project-sunbird/logger";
import { health as postgresHealth } from "../connections/databaseConnection";
import { HealthStatus } from "../types/DatasetModels";
import { createClient } from 'redis';
import { isHealthy as isKafkaHealthy } from "../connections/kafkaConnection";
import { druidHttpService, executeNativeQuery } from "../connections/druidConnection";
import _ from "lodash";
import moment from "moment";

const dateFormat = 'YYYY-MM-DDT00:00:00+05:30'

const prometheusInstance = axios.create({ baseURL: config?.query_api?.prometheus?.url, headers: { "Content-Type": "application/json" } });
let isRedisDenormHealthy = false;
let isRedisDedupHealthy = false;
const init = async () => {
  createClient({
    url: `redis://${config.redis_config.denorm_redis_host}:${config.redis_config.denorm_redis_port}`
  })
    .on('error', err => {
      logger.error('unable to connect to denorm redis client', err)
      isRedisDenormHealthy = false
    })
    .on("ready", () => {
      isRedisDenormHealthy = true
    })
    .connect();

  createClient({
    url: `redis://${config.redis_config.dedup_redis_host}:${config.redis_config.dedup_redis_port}`
  })
    .on("ready", () => {
      isRedisDedupHealthy = true
    })
    .on('error', err => {
      isRedisDedupHealthy = false
      logger.error('unable to connect to dedup redis client', err)
    })
    .connect();
}


const queryMetrics = (params: Record<string, any> | string) => {
  return prometheusInstance.get("/api/v1/query", { params })
}

export const getInfraHealth = async (): Promise<{components: any, status: string}> => {
  const postgres = await getPostgresStatus() 
  const redis = await getRedisStatus()
  const kafka = await getKafkaHealthStatus()
  const druid = await getDruidHealthStatus()
  const flink = await getFlinkHealthStaus()
  const components =  [{"type": "postgres", "status": postgres},
    {"type": "redis", "status": redis},
    {"type": "kafka", "status": kafka},
    {"type": "druid", "status": druid},
    {"type": "flink", "status": flink},
  ]
  const status = [postgres, redis, kafka, druid, flink].includes(HealthStatus.UnHealthy) ? HealthStatus.UnHealthy : HealthStatus.Healthy
  return {components, status};
}
export const getPostgresStatus = async (): Promise<HealthStatus> => {
  try {
    const postgresStatus = await postgresHealth()
    logger.debug(postgresStatus)
  } catch (error) {
    logger.error('errr: ', error)
    return HealthStatus.UnHealthy
  }
  return HealthStatus.Healthy
}

export const getRedisStatus = async () => {
  return isRedisDenormHealthy && isRedisDedupHealthy ? HealthStatus.Healthy : HealthStatus.UnHealthy
}

export const getKafkaHealthStatus = async () => {
  try {
    const status = await isKafkaHealthy()
    return status ? HealthStatus.Healthy : HealthStatus.UnHealthy
  } catch (error) {
    return HealthStatus.UnHealthy
  }

}

export const getFlinkHealthStaus = async () => {
  try {
    const responses = await Promise.all(
      [axios.get(config?.flink_job_configs?.masterdata_processor_job_manager_url as string + "/jobs"),
      axios.get(config?.flink_job_configs?.pipeline_merged_job_manager_url as string + "/jobs")]
    )
    const isHealthy = _.every(responses, (response: any) => {
      const { data = {} } = response;
      return _.get(data, "jobs[0].status") === "RUNNING"
    })
    return isHealthy ? HealthStatus.Healthy : HealthStatus.UnHealthy;
  } catch (error) {
    logger.error("Unable to get flink status", error)
  }
  return HealthStatus.UnHealthy;
}

export const getDruidHealthStatus = async () => {
  try {
    const { data = false } = await druidHttpService.get("/status/health")
    return data ? HealthStatus.Healthy : HealthStatus.UnHealthy
  } catch (error) {
    logger.error("druid health check", error)
    return HealthStatus.UnHealthy
  }
}

export const getEventsProcessedToday = async (datasetId: string, isMaster: boolean) => {
  const startDate = moment().format(dateFormat);
  const endDate = moment().add(1, 'd').format(dateFormat);

  const intervals = `${startDate}/${endDate}`
  const { data } = await executeNativeQuery({
    "queryType": "timeseries",
    "dataSource": "system-events",
    "intervals": intervals,
    "granularity": {
      "type": "all",
      "timeZone": "Asia/Kolkata"
    },
    "filter": {
      "type": "and",
      "fields": [
        {
          "type": "selector",
          "dimension": "ctx_module",
          "value": "processing"
        },
        {
          "type": "selector",
          "dimension": "ctx_dataset",
          "value": datasetId
        },
        {
          "type": "selector",
          "dimension": "ctx_pdata_id",
          "value": isMaster ? "MasterDataProcessorJob" : "DruidRouterJob"
        },
        {
          "type": "selector",
          "dimension": "error_code",
          "value": null
        }
      ]
    },
    "aggregations": [
      {
        "type": "longSum",
        "name": "count",
        "fieldName": "count"
      }
    ]
  })
  console.log()
}

init().catch(err => logger.error(err))