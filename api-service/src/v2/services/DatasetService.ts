import _ from "lodash";
import logger from "../logger";
import { cipherService } from "./CipherService";
import { defaultDatasetConfig } from "../configs/DatasetConfigDefault";
import { Dataset } from "../models/Dataset";
import { DatasetDraft } from "../models/DatasetDraft";
import { DatasetTransformations } from "../models/Transformation";
import { DatasetTransformationsDraft } from "../models/TransformationDraft";


class DatasetService {

    getDataset = async (datasetId: string, raw = false): Promise<any> => {
        const dataset = await Dataset.findOne({
            where: {
                id: datasetId,
            },
            raw: raw
        });
        return dataset
    }

    getDuplicateDenormKey = (denormConfig: Record<string, any>): Array<string> => {
        if (denormConfig && _.isArray(_.get(denormConfig, "denorm_fields"))) {
            const denormFields = _.get(denormConfig, "denorm_fields")
            const denormOutKeys = _.map(denormFields, field => _.get(field, "denorm_out_field"))
            const duplicateDenormKeys: Array<string> = _.filter(denormOutKeys, (item: string, index: number) => _.indexOf(denormOutKeys, item) !== index);
            return duplicateDenormKeys;
        }
        return []
    }

    checkDatasetExists = async (dataset_id: string): Promise<boolean> => {
        const draft = await DatasetDraft.findOne({ where: { dataset_id }, attributes:["id"], raw: true });
        if (draft === null) {
            const live = await Dataset.findOne({ where: { id: dataset_id }, attributes:["id"], raw: true });
            return !(live === null)
        } else {
            return true;
        }
    }

    getDraftDataset = async (dataset_id: string) => {
        return DatasetDraft.findOne({ where: { dataset_id }, raw: true });
    }

    getDraftTransformations = async (dataset_id: string) => {
        return DatasetTransformationsDraft.findAll({ where: { dataset_id }, raw: true });
    }

    getTransformations = async (dataset_id: string) => {
        return DatasetTransformations.findAll({ where: { dataset_id }, raw: true });
    }

    createDataset = async (datasetReq: Record<string, any>): Promise<Record<string, any>> => {
        const transformationsConfig:Array<Record<string, any>> = _.get(datasetReq, "transformations_config")
        const connectorsConfig:Array<Record<string, any>> = _.get(datasetReq, "connectors_config")
        const dataset = _.omit(datasetReq, ["transformations_config", "connectors_config"])
        const mergedDataset = this.mergeDatasetConfigs(defaultDatasetConfig, dataset)
        const draftDataset = { 
            ...mergedDataset, 
            version_key: Date.now().toString(),
            transformations_config: this.getDatasetTransformations(transformationsConfig),
            connectors_config: this.getDatasetConnectors(connectorsConfig),
        }
        const response = await DatasetDraft.create(draftDataset)
        const responseData = { id: _.get(response, ["dataValues", "id"]) || "", version_key: draftDataset.version_key }
        logger.info({ datasetReq, message: `Dataset Created Successfully with id:${_.get(response, ["dataValues", "id"])}`, response: responseData })
        return responseData
    }

    mergeDatasetConfigs = (defaultConfig: Record<string, any>, requestPayload: Record<string, any>): Record<string, any> => {
        const { id, dataset_id } = requestPayload;
        const datasetId = !id ? dataset_id : id
        const modifyPayload = { ...requestPayload, id: datasetId, router_config: { topic: datasetId } }
        const defaults = _.cloneDeep(defaultConfig)
        const datasetConfigs = _.merge(defaults, modifyPayload)
        return datasetConfigs
    }

    getDatasetConnectors = (connectorConfigs: Array<Record<string, any>>): Array<Record<string, any>> => {
        
        if (!_.isEmpty(connectorConfigs)) {
            const uniqueConnectors = _.uniqWith(connectorConfigs, (a: Record<string, any>, b: Record<string, any>) => {
                return _.isEqual(a.connector_id, b.connector_id) && _.isEqual(a.connector_config, b.connector_config)
            })
            return _.map(uniqueConnectors, (config) => {
                return {
                    connector_id: config.connector_id,
                    connector_config: cipherService.encrypt(JSON.stringify(config.connector_config)),
                    operations_config: config.operations_config
                }
            })
        }
        return []
    }

    getDatasetTransformations = (configs: Array<Record<string, any>>): Array<Record<string, any>> => {

        if (configs) {
            return _.uniqBy(configs, "field_key")
        }
        return []
    }

}

export const datasetService = new DatasetService();