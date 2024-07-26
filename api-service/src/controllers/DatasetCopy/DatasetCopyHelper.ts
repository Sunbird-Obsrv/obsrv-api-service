import * as _ from "lodash";
import { DatasetStatus } from "../../types/DatasetModels";
import { defaultDatasetConfig } from "../../configs/DatasetConfigDefault";
import { query } from "../../connections/databaseConnection";
import { config } from "../../configs/Config";
const version = defaultDatasetConfig.version;

export const updateRecords = (datasetRecord: Record<string, any>, newDatasetId: string): void => {
    const dataset_id = newDatasetId;
    _.set(datasetRecord, 'api_version', "v2")
    _.set(datasetRecord, 'status', DatasetStatus.Draft)
    _.set(datasetRecord, "dataset_id", dataset_id)
    _.set(datasetRecord, "id", dataset_id)
    _.set(datasetRecord, "name", dataset_id)
    _.set(datasetRecord, "version_key", Date.now().toString())
    _.set(datasetRecord, 'version', version);
    _.set(datasetRecord, "entry_topic", config.telemetry_service_config.kafka.topics.createDataset)
    _.set(datasetRecord, "router_config", { topic: newDatasetId })

    if (datasetRecord?.type === "master") {
        _.set(datasetRecord, "dataset_config.cache_config.redis_db", updateMasterDatasetConfig(datasetRecord?.dataset_config))
    }
}

const updateMasterDatasetConfig = async (datasetConfig: any) => {
    let nextRedisDB = datasetConfig.redis_db;
    const { results }: any = await query("SELECT nextval('redis_db_index')")
    if (!_.isEmpty(results)) nextRedisDB = parseInt(_.get(results, "[0].nextval")) || 3;
    return _.assign(datasetConfig, { "redis_db": nextRedisDB })
}
