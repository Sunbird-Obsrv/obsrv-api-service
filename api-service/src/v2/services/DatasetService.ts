import _ from "lodash";
import logger from "../logger";
import { Dataset } from "../models/Dataset";
import { DatasetDraft } from "../models/DatasetDraft";
import { DatasetTransformations } from "../models/Transformation";
import { DatasetTransformationsDraft } from "../models/TransformationDraft";
import Model from "sequelize/types/model";
import { DatasetSourceConfigDraft } from "../models/DatasetSourceConfigDraft";
import { query, sequelize } from "../connections/databaseConnection";
import { DatasetSourceConfig } from "../models/DatasetSourceConfig";
import { ConnectorInstances } from "../models/ConnectorInstances";
import { DatasourceDraft } from "../models/DatasourceDraft";
import { executeCommand } from "../connections/commandServiceConnection";
import Transaction from "sequelize/types/transaction";
import { DatasetStatus, DatasetType } from "../types/DatasetModels";
import { Datasource } from "../models/Datasource";
import { obsrvError } from "../types/ObsrvError";
import { druidHttpService } from "../connections/druidConnection";
import { tableGenerator } from "./TableGenerator";

class DatasetService {

    getDataset = async (datasetId: string, attributes?: string[], raw = false): Promise<any> => {
        return Dataset.findOne({ where: { id: datasetId }, attributes, raw: raw });
    }

    findDatasets = async (where?: Record<string, any>, attributes?: string[], order?: any): Promise<any> => {
        return Dataset.findAll({where, attributes, order, raw: true})
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

    getDraftDataset = async (dataset_id: string, attributes?: string[]) => {
        return DatasetDraft.findOne({ where: { dataset_id }, attributes, raw: true });
    }

    findDraftDatasets = async (where?: Record<string, any>, attributes?: string[], order?: any): Promise<any> => {
        return DatasetDraft.findAll({where, attributes, order, raw: true})
    }

    getDraftTransformations = async (dataset_id: string, attributes?: string[]) => {
        return DatasetTransformationsDraft.findAll({ where: { dataset_id }, attributes, raw: true });
    }

    getDraftConnectors = async (dataset_id: string, attributes?: string[]) => {
        return DatasetSourceConfigDraft.findAll({ where: { dataset_id }, attributes, raw: true });
    }

    getConnectorsV1 = async (dataset_id: string, attributes?: string[]) => {
        return DatasetSourceConfig.findAll({ where: { dataset_id }, attributes, raw: true });
    }

    getConnectors = async (dataset_id: string, attributes?: string[]) => {
        return ConnectorInstances.findAll({ where: { dataset_id }, attributes, raw: true });
    }

    getTransformations = async (dataset_id: string, attributes?: string[]) => {
        return DatasetTransformations.findAll({ where: { dataset_id }, attributes, raw: true });
    }

    updateDraftDataset = async (draftDataset: Record<string, any>): Promise<Record<string, any>> => {

        await DatasetDraft.update(draftDataset, { where: { id: draftDataset.id }});
        const responseData = { message: "Dataset is updated successfully", id: draftDataset.id, version_key: draftDataset.version_key };
        logger.info({ draftDataset, message: `Dataset updated successfully with id:${draftDataset.id}`, response: responseData });
        return responseData;
    }

    createDraftDataset = async (draftDataset: Record<string, any>): Promise<Record<string, any>> => {

        const response = await DatasetDraft.create(draftDataset);
        const responseData = { id: _.get(response, ["dataValues", "id"]) || "", version_key: draftDataset.version_key };
        logger.info({ draftDataset, message: `Dataset Created Successfully with id:${_.get(response, ["dataValues", "id"])}`, response: responseData });
        return responseData;
    }

    migrateDraftDataset = async (datasetId: string, dataset: Model<any, any>): Promise<any> => {

        let draftDataset : Record<string, any> = {
            api_version: "v2"
        }
        const dataset_config:any = _.get(dataset, "dataset_config");
        draftDataset["dataset_config"] = {
            indexing_config: {olap_store_enabled: true, lakehouse_enabled: false, cache_enabled: (_.get(dataset, "type") === "master")},
            keys_config: {data_key: dataset_config.data_key, timestamp_key: dataset_config.timestamp_key},
            cache_config: {redis_db_host: dataset_config.redis_db_host, redis_db_port: dataset_config.redis_db_port, redis_db: dataset_config.redis_db}
        }
        const transformations = await this.getDraftTransformations(datasetId, ["field_key", "transformation_function", "mode", "metadata"]);
        draftDataset["transformations_config"] = _.map(transformations, (config) => {
            return {
                field_key: _.get(config, ["field_key"]),
                transformation_function: _.get(config, ["transformation_function"]),
                mode: _.get(config, ["mode"]),
                datatype: _.get(config, ["metadata._transformedFieldDataType"]) || "string",
                category: this.getTransformationCategory(_.get(config, ["metadata.section"]))
            }
        })
        const connectors = await this.getDraftConnectors(datasetId, ["connector_type", "connector_config"]);
        draftDataset["connectors_config"] = _.map(connectors, (config) => {
            return {
                connector_id: _.get(config, ["connector_type"]),
                connector_config: _.get(config, ["connector_config"]),
                version: "v1"
            }
        })
        
        const transaction = await sequelize.transaction();
        try {
            await DatasetDraft.update(draftDataset, { where: { id: datasetId }, transaction});
            await DatasetTransformationsDraft.destroy({ where: { dataset_id: datasetId }, transaction });
            await DatasetSourceConfigDraft.destroy({ where: { dataset_id: datasetId }, transaction });
            await transaction.commit();
        } catch (err) {
            await transaction.rollback();
            throw err;
        }
        return await this.getDraftDataset(datasetId);
    }

    getTransformationCategory = (section: string):string => {

        switch(section) {
            case "pii":
                return "pii";
            case "additionalFields":
                return "derived";
            default:
                return "transform";
        }
    }

    createDraftDatasetFromLive = async (dataset: Model<any, any>) => {
        
        let draftDataset:any = _.omit(dataset, ["created_date", "updated_date", "published_date"]);
        const dataset_config:any = _.get(dataset, "dataset_config");
        const api_version:any = _.get(dataset, "api_version");
        if(api_version === "v1") {
            draftDataset["dataset_config"] = {
                indexing_config: {olap_store_enabled: true, lakehouse_enabled: false, cache_enabled: (_.get(dataset, "type") === "master")},
                keys_config: {data_key: dataset_config.data_key, timestamp_key: dataset_config.timestamp_key},
                cache_config: {redis_db_host: dataset_config.redis_db_host, redis_db_port: dataset_config.redis_db_port, redis_db: dataset_config.redis_db}
            }
            const connectors = await this.getConnectorsV1(draftDataset.dataset_id, ["connector_type", "connector_config"]);
            draftDataset["connectors_config"] = _.map(connectors, (config) => {
                return {
                    connector_id: _.get(config, "connector_type"),
                    connector_config: _.get(config, "connector_config"),
                    version: "v1"
                }
            })
            const transformations = await this.getDraftTransformations(draftDataset.dataset_id, ["field_key", "transformation_function", "mode", "metadata"]);
            draftDataset["transformations_config"] = _.map(transformations, (config) => {
                return {
                    field_key: _.get(config, "field_key"),
                    transformation_function: _.get(config, "transformation_function"),
                    mode: _.get(config, "mode"),
                    datatype: _.get(config, "metadata._transformedFieldDataType") || "string",
                    category: this.getTransformationCategory(_.get(config, ["metadata.section"]))
                }
            })
            draftDataset["api_version"] = "v2"
        } else {
            const connectors = await this.getConnectors(draftDataset.dataset_id, ["connector_id", "connector_config", "operations_config"]);
            draftDataset["connectors_config"] = connectors
            const transformations = await this.getTransformations(draftDataset.dataset_id, ["field_key", "transformation_function", "mode", "datatype", "category"]);
            draftDataset["transformations_config"] = transformations
        }
        draftDataset["version_key"] = Date.now().toString()
        draftDataset["version"] = _.add(_.get(dataset, ["version"]), 1); // increment the dataset version
        await DatasetDraft.create(draftDataset);
        return await this.getDraftDataset(draftDataset.dataset_id);
    }

    getNextRedisDBIndex = async () => {
        return await query("SELECT nextval('redis_db_index')")
    }

    deleteDraftDataset = async (dataset: Record<string, any>) => {

        const { id } = dataset
        const transaction = await sequelize.transaction()
        try {
            await DatasetTransformationsDraft.destroy({ where: { dataset_id: id } , transaction})
            await DatasetSourceConfigDraft.destroy({ where: { dataset_id: id } , transaction})
            await DatasourceDraft.destroy({ where: { dataset_id: id } , transaction})
            await DatasetDraft.destroy({ where: { id } , transaction})
            await transaction.commit()
        } catch (err:any) {
            await transaction.rollback()
            throw obsrvError(dataset.id, "FAILED_TO_DELETE_DATASET", err.message, "SERVER_ERROR", 500, err)
        }
    }

    retireDataset = async (dataset: Record<string, any>) => {

        const transaction = await sequelize.transaction();
        try {
            await Dataset.update({ status: DatasetStatus.Retired }, { where: { id: dataset.id }, transaction });
            await DatasetSourceConfig.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id }, transaction });
            await Datasource.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id } , transaction});
            await DatasetTransformations.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id } , transaction});
            await transaction.commit();
            await this.deleteDruidSupervisors(dataset);
        } catch(err:any) {
            await transaction.rollback();
            throw obsrvError(dataset.id, "FAILED_TO_RETIRE_DATASET", err.message, "SERVER_ERROR", 500, err);
        }
    }

    deleteDruidSupervisors = async (dataset: Record<string, any>) => {

        try {
            if (dataset.type !== DatasetType.master) {
                const datasourceRefs = await Datasource.findAll({ where: { dataset_id: dataset.id }, attributes: ["datasource_ref"], raw: true })
                for (const sourceRefs of datasourceRefs) {
                    const datasourceRef = _.get(sourceRefs, "datasource_ref")
                    await druidHttpService.post(`/druid/indexer/v1/supervisor/${datasourceRef}/terminate`)
                    logger.info(`Datasource ref ${datasourceRef} deleted from druid`)
                }
            }
        } catch (error: any) {
            logger.error({ error: _.get(error, "message"), message: `Failed to delete supervisors for dataset:${dataset.id}` })
        }
    }

    publishDataset = async (draftDataset: Record<string, any>) => {

        const transaction = await sequelize.transaction()
        try {
            await DatasetDraft.update(draftDataset, { where: { id: draftDataset.id } , transaction})
            await this.generateDataSouce(draftDataset, transaction)
            await transaction.commit()
            await executeCommand(draftDataset.id, "PUBLISH_DATASET");
        } catch(err:any) {
            await transaction.rollback()
            throw obsrvError(draftDataset.id, "FAILED_TO_PUBLISH_DATASET", err.message, "SERVER_ERROR", 500, err);
        }
        
    }

    generateDataSouce = async (draftDataset: Record<string, any>, transaction: Transaction) => {

        const indexingConfig = draftDataset.dataset_config.indexing_config;
        if(indexingConfig.olap_store_enabled) {
            const draftDatasource = this.createDraftDatasource(draftDataset, "druid");
            const ingestionSpec = tableGenerator.getDruidIngestionSpec(draftDataset, draftDatasource.datasource_ref);
            _.set(draftDatasource, 'ingestion_spec', ingestionSpec)
            await DatasourceDraft.create(draftDatasource, {transaction})
        }
        if(indexingConfig.lakehouse_enabled) {

        }
    }

    createDraftDatasource = (draftDataset: Record<string, any>, type: string) : Record<string, any> => {

        const datasource = _.join([draftDataset.dataset_id,"events"], "_")
        return {
            id: datasource,
            datasource: draftDataset.dataset_id,
            dataset_id: draftDataset.dataset_id,
            datasource_ref: datasource,
            type
        }
    }

}

export const datasetService = new DatasetService();