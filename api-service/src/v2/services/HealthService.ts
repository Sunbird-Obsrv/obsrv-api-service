import axios from "axios";
import { config } from "../configs/Config";
import { logger } from "@project-sunbird/logger";
import { health as postgresHealth } from "../connections/databaseConnection";
import { HealthStatus } from "../types/DatasetModels";
import { createClient } from 'redis';

const prometheusInstance = axios.create({ baseURL: config?.query_api?.prometheus?.url, headers: { "Content-Type": "application/json" } });
let redisDenormClient: any;
let redisDedupClient: any;
const init = async () => {
  redisDenormClient = await createClient({
    url: `redis://${config.redis_config.denorm_redis_host}:${config.redis_config.denorm_redis_port}`
  })
  .on('error', err => logger.error('unable to connect to denorm redis client', err))
  .connect();
  redisDedupClient = await createClient({
    url: `redis://${config.redis_config.dedup_redis_host}:${config.redis_config.dedup_redis_port}`
  })
  .on('error', err => logger.error('unable to connect to dedup redis client', err))
  .connect();
}


const queryMetrics = (params: Record<string, any> | string) => {
  return prometheusInstance.get("/api/v1/query", { params })
}
export const getPostgresStatus = async (): Promise<HealthStatus> => {
  try {
    const postgresStatus = await postgresHealth()
    logger.debug(postgresStatus)
  } catch (error) {
    return HealthStatus.UnHealthy
  }
  return HealthStatus.Healthy
}

export const getRedisStatus = async () => {
  try {
    await Promise.all([redisDenormClient.ping(), redisDedupClient.ping()])  
  } catch (error) {
    return HealthStatus.UnHealthy
  }
  return HealthStatus.Healthy
}

init().catch(err => logger.error(err))