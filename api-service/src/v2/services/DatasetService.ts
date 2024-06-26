import { Dataset } from "../models/Dataset";
import _ from "lodash";
import { DatasetDraft } from "../models/DatasetDraft";
import { DatasetTransformationsDraft } from "../models/TransformationDraft";
import { Request } from "express";
import { generateIngestionSpec } from "./IngestionService";
import { ingestionConfig } from "../configs/IngestionConfig";
import { DatasetTransformations } from "../models/Transformation";
import { getUpdatedSchema } from "./DatasourceService";
import { DatasetType } from "../types/DatasetModels";
import { defaultDatasetConfig, defaultMasterConfig } from "../configs/DatasetConfigDefault";
import { query } from "../connections/databaseConnection";
import { ErrorObject } from "../types/ResponseModel";

export const getDataset = async (datasetId: string, raw = false): Promise<any> => {
    const dataset = await Dataset.findOne({
        where: {
            id: datasetId,
        },
        raw: raw
    });
    return dataset
}

export const getDuplicateDenormKey = (denormConfig: Record<string, any>): Array<string> => {
    if (denormConfig && _.isArray(_.get(denormConfig, "denorm_fields"))) {
        const denormFields = _.get(denormConfig, "denorm_fields")
        const denormOutKeys = _.map(denormFields, field => _.get(field, "denorm_out_field"))
        const duplicateDenormKeys: Array<string> = _.filter(denormOutKeys, (item: string, index: number) => _.indexOf(denormOutKeys, item) !== index);
        return duplicateDenormKeys;
    }
    return []
}

export const getDraftDataset = async (dataset_id: string) => {
    return DatasetDraft.findOne({ where: { dataset_id }, raw: true });
}

export const getDraftTransformations = async (dataset_id: string) => {
    return DatasetTransformationsDraft.findAll({ where: { dataset_id }, raw: true });
}

export const getTransformations = async (dataset_id: string) => {
    return DatasetTransformations.findAll({ where: { dataset_id }, raw: true });
}

export const setReqDatasetId = (req: Request, dataset_id: string) => {
    if (dataset_id) {
        return _.set(req, "dataset_id", dataset_id)
    }
}

export const getDuplicateConfigs = (configs: Array<string | any>) => {
    return _.filter(configs, (item: string, index: number) => _.indexOf(configs, item) !== index);
}

export const generateDataSource = async (payload: Record<string, any>) => {
    const { id } = payload
    const updatedSchema = await getUpdatedSchema(payload)
    const ingestionSpec = generateIngestionSpec({ ...payload, data_schema: updatedSchema })
    const dataSource = getDataSource({ ingestionSpec, id })
    return dataSource
}

const getDataSource = (ingestionPayload: Record<string, any>) => {
    const { ingestionSpec, id } = ingestionPayload
    const dataSource = `${id}_${_.toLower(ingestionConfig.granularitySpec.segmentGranularity)}`
    const dataSourceId = `${id}_${dataSource}`
    return {
        id: dataSourceId,
        datasource: dataSource,
        dataset_id: id,
        ingestion_spec: ingestionSpec,
        datasource_ref: dataSource
    }
}

const getDatasetDefaults = async (payload: Record<string, any>): Promise<Record<string, any>> => {
    const datasetPayload = mergeDatasetConfigs(defaultDatasetConfig, payload)
    const denormPayload = await updateDenormFields(_.get(datasetPayload, "denorm_config"))
    return { ...datasetPayload, denorm_config: denormPayload }
}

const setRedisDBConfig = async (datasetConfig: Record<string, any>): Promise<Record<string, any>> => {
    let nextRedisDB = datasetConfig.redis_db;
    const { results }: any = await query("SELECT nextval('redis_db_index')")
    if (!_.isEmpty(results)) nextRedisDB = parseInt(_.get(results, "[0].nextval")) || 3;
    return _.assign(datasetConfig, { "redis_db": nextRedisDB })
}

const getMasterDatasetDefaults = async (payload: Record<string, any>): Promise<Record<string, any>> => {
    const masterDatasetPayload = mergeDatasetConfigs(defaultMasterConfig, payload)
    let datasetConfig = masterDatasetPayload.dataset_config
    datasetConfig = await setRedisDBConfig(datasetConfig);
    return _.assign(masterDatasetPayload, datasetConfig);
}

const getDefaultHandler = (datasetType: string) => {
    if (datasetType == DatasetType.Dataset) {
        return getDatasetDefaults;
    } else {
        return getMasterDatasetDefaults;
    }
}

export const getDefaultValue = async (payload: Record<string, any>) => {
    const datasetType = _.get(payload, "type");
    const getDatasetDefaults = getDefaultHandler(datasetType)
    const datasetDefaults = await getDatasetDefaults(payload)
    return _.omit(datasetDefaults, ["transformations_config"])
}

const mergeDatasetConfigs = (defaultConfig: Record<string, any>, requestPayload: Record<string, any>): Record<string, any> => {
    const { id, dataset_id } = requestPayload;
    const datasetId = !id ? dataset_id : id
    const modifyPayload = { ...requestPayload, id: datasetId, router_config: { topic: datasetId } }
    const defaults = _.cloneDeep(defaultConfig)
    const datasetConfigs = _.merge(defaults, modifyPayload)
    return datasetConfigs
}

const updateDenormFields = async (denormConfigs: Record<string, any>) => {
    const denormFields = _.get(denormConfigs, "denorm_fields")
    if (_.isEmpty(denormFields)) {
        return denormConfigs
    }
    const masterDatasets = await Dataset.findAll({ where: { type: DatasetType.MasterDataset }, raw: true });
    const updatedFields = _.map(denormFields, fields => {
        const master = _.find(masterDatasets, dataset => _.get(dataset, ["dataset_id"]) === fields.dataset_id)
        if (_.isEmpty(master)) {
            throw {
                code: "DATASET_DENORM_NOT_FOUND",
                message: "Denorm Master dataset not found",
                statusCode: 404,
                errCode: "NOT_FOUND"
            } as ErrorObject
        }
        const redis_db = _.get(master, ["dataset_config", "redis_db"])
        return { ...fields, redis_db }
    })
    return { ...denormConfigs, denorm_fields: updatedFields }
}