import _ from "lodash";
import { v4 } from "uuid";
import logger from "../logger";
import { cipherService } from "./CipherService";
import { sequelize } from "../connections/databaseConnection";
import { defaultDatasetConfig } from "../configs/DatasetConfigDefault";
import { Dataset } from "../models/Dataset";
import { DatasetDraft } from "../models/DatasetDraft";
import { DatasetTransformations } from "../models/Transformation";
import { ConnectorInstancesDraft } from "../models/ConnectorInstancesDraft";
import { DatasetTransformationsDraft } from "../models/TransformationDraft";
import { DatasetSourceConfigDraft } from "../models/DatasetSourceConfigDraft";

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

    create = async (datasetReq: Record<string, any>): Promise<Record<string, any>> => {

        let transaction;
        try {
            transaction = await sequelize.transaction()
            const dataset = await this.createDataset(datasetReq, transaction);
            await this.createDatasetTransformations(datasetReq, dataset.id, transaction)
            await this.createConnectorInstances(datasetReq, dataset.id, transaction)
            await transaction.commit()
            return dataset
        } catch(err) {
            transaction && await transaction.rollback()
            throw err
        }
        
    }

    createDataset = async (datasetReq: Record<string, any>, transaction: any): Promise<Record<string, any>> => {
        const dataset = _.omit(datasetReq, ["transformations_config", "connector_config"])
        const mergedDataset = this.mergeDatasetConfigs(defaultDatasetConfig, dataset)
        const data = { ...mergedDataset, version_key: Date.now().toString() }
        const response = await DatasetDraft.create(data, {transaction: transaction})
        const responseData = { id: _.get(response, ["dataValues", "id"]) || "", version_key: data.version_key }
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

    createDatasetTransformations = async (dataset: Record<string, any>, datasetId: string, transaction: any) => {
        const transformationConfig: any = this.getTransformationConfig({ transformationPayload: _.get(dataset, "transformations_config"), datasetId: datasetId })
        if (!_.isEmpty(transformationConfig)) {
            await DatasetTransformationsDraft.bulkCreate(transformationConfig, {transaction: transaction});
            logger.info({ transformationConfig, message: `Dataset transformations records created successsfully for dataset:${datasetId}` })
        }
    }

    createConnectorInstances = async (dataset: Record<string, any>, datasetId: string, transaction: any) => {
        
        const connectorConfigs = _.get(dataset, "connectors_config")
        if (!_.isEmpty(connectorConfigs)) {
            const uniqueConnectors = _.uniqWith(connectorConfigs, (a: Record<string, any>, b: Record<string, any>) => {
                return _.isEqual(a.connector_id, b.connector_id) && _.isEqual(a.connector_config, b.connector_config)
            })
            const connectorsToPersist = _.map(uniqueConnectors, (config) => {
                return {
                    id: v4(),
                    dataset_id: datasetId,
                    connector_id: config.connector_id,
                    connector_config: cipherService.encrypt(JSON.stringify(config.connector_config)),
                    operations_config: config.operations_config
                }
            })
            const v1DataSources = _.map(_.filter(connectorsToPersist, (conn) => {
                return _.includes(["kafka-connector", "debezium-connector", "sb-knowlg-connector"], conn.connector_id)
            }), (config) => {
                return {
                    id: v4(),
                    dataset_id: datasetId,
                    connector_type: this.getConnectorTypeV1(config.connector_id),
                    connector_config: this.getConnectorConfigV1(config.connector_config),
                    operations_config: config.operations_config
                }
            })
            await ConnectorInstancesDraft.bulkCreate(connectorsToPersist, {transaction: transaction});
            await DatasetSourceConfigDraft.bulkCreate(v1DataSources, {transaction: transaction});
            logger.info({ uniqueConnectors, message: `Connector instances created successsfully for dataset:${datasetId}` })
        }
    }

    getConnectorTypeV1 = (connector_id: string) : string => {
        switch(connector_id) {
            case "debezium-connector": 
                return "debezium"
            case "sb-knowlg-connector":
                return "neo4j"
            default:
                return "kafka"
        }
    }

    getConnectorConfigV1 = (connector_config: any): Record<string, any> => {
        return {
            "topic": connector_config.source_kafka_topic,
            "kafkaBrokers": connector_config.source_kafka_broker_servers
        }
    }

    getTransformationConfig = (configs: Record<string, any>): Record<string, any> => {

        const { transformationPayload, datasetId } = configs
        if (transformationPayload) {
    
            let transformations: any = []
            const transformationFieldKeys = _.flatten(_.map(transformationPayload, fields => _.get(fields, ["field_key"])))
            const duplicateFieldKeys: Array<string> = this.getDuplicateConfigs(transformationFieldKeys)
    
            if (!_.isEmpty(duplicateFieldKeys)) {
                logger.info({ message: `Duplicate transformations provided by user are [${duplicateFieldKeys}]` })
            }
    
            _.forEach(transformationPayload, payload => {
                const fieldKey = _.get(payload, "field_key")
                const transformationExists = _.some(transformations, field => _.get(field, "field_key") == fieldKey)
                if (!transformationExists) {
                    transformations = _.flatten(_.concat(transformations, { ...payload, id: `${datasetId}_${fieldKey}`, dataset_id: datasetId }))
                }
            })
            return transformations
        }
        return []
    }

    getDuplicateConfigs = (configs: Array<string | any>) => {
        return _.filter(configs, (item: string, index: number) => _.indexOf(configs, item) !== index);
    }
}

export const datasetService = new DatasetService();