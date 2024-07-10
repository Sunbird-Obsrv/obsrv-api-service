import { Request, Response } from "express";
import _ from "lodash";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { datasetService } from "../../services/DatasetService";
import { ErrorObject } from "../../types/ResponseModel";
import { schemaValidation } from "../../services/ValidationService";
import StatusTransitionSchema from "./RequestValidationSchema.json";
import ReadyToPublishSchema from "./ReadyToPublishSchema.json"
import httpStatus from "http-status";
import { DatasetTransformationsDraft } from "../../models/TransformationDraft";
import { DatasourceDraft } from "../../models/DatasourceDraft";
import { DatasetSourceConfigDraft } from "../../models/DatasetSourceConfigDraft";
import { DatasetDraft } from "../../models/DatasetDraft";
import { Dataset } from "../../models/Dataset";
import { DatasetAction, DatasetStatus, DatasetType } from "../../types/DatasetModels";
import { DatasetSourceConfig } from "../../models/DatasetSourceConfig";
import { Datasource } from "../../models/Datasource";
import { DatasetTransformations } from "../../models/Transformation";
import { executeCommand } from "../../connections/commandServiceConnection";
import { druidHttpService } from "../../connections/druidConnection";
import { query, sequelize } from "../../connections/databaseConnection";
import { defaultDatasetConfig } from "../../configs/DatasetConfigDefault";

export const apiId = "api.datasets.status-transition";
const transitionFailed = "DATASET_STATUS_TRANSITION_FAILURE"
const invalidRequest = "DATASET_STATUS_TRANSITION_INVALID_INPUT"
const datasetNotFound = "DATASET_NOT_FOUND"

const allowedTransitions: Record<string, any> = {
    Delete: [DatasetStatus.Draft, DatasetStatus.ReadyToPublish],
    ReadyToPublish: [DatasetStatus.Draft],
    Live: [DatasetStatus.ReadyToPublish],
    Retire: [DatasetStatus.Live],
    Archive: [DatasetStatus.Retired],
    Purge: [DatasetStatus.Archived]
}
const liveDatasetActions = ["Retire", "Archive", "Purge"]

const statusTransitionCommands = {
    Delete: ["DELETE_DRAFT_DATASETS"],
    ReadyToPublish: ["VALIDATE_DATASET_CONFIGS"],
    Live: ["PUBLISH_DATASET"],
    Retire: ["CHECK_DATASET_IS_DENORM", "SET_DATASET_TO_RETIRE", "DELETE_SUPERVISORS", "RESTART_PIPELINE"]
}

const logHeaders = (req: Request, res: Response) => {
    return {
        apiId: "api.datasets.status-transition", msgid:_.get(req, ["body", "params", "msgid"]), request: req.body, resmsgid: _.get(res, "resmsgid")
    }
}

const validateRequest =  (req: Request, res: Response, headers: Record<string, any>): boolean => {
    const isRequestValid: Record<string, any> = schemaValidation(req.body, StatusTransitionSchema)
    if (!isRequestValid.isValid) {
        logger.error({ code: invalidRequest, headers, message: isRequestValid.message })
        ResponseHandler.errorResponse({
            code: invalidRequest,
            message: isRequestValid.message,
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }
    return true;
}

const validateDataset = (req: Request, res: Response, dataset: any, action: string, headers: Record<string, any>) : boolean => {
    
    if (_.isEmpty(dataset)) {
        logger.error({ code: datasetNotFound, headers, message: `Dataset not found for dataset:${dataset.id}` })
        ResponseHandler.errorResponse({
            code: datasetNotFound,
            message: `Dataset not found for dataset: ${dataset.id}`,
            statusCode: 404,
            errCode: "NOT_FOUND"
        } as ErrorObject, req, res);
        return false;
    }

    if(!_.includes(allowedTransitions[action], dataset.status)) {
        const code = `DATASET_${_.toUpper(action)}_FAILURE`
        logger.error({ code, headers, message: `for dataset: ${dataset.id} status:${dataset.status} with status transition to ${action}` })
        ResponseHandler.errorResponse({
            code: datasetNotFound,
            message: `$ for dataset: ${dataset.id} status:${dataset.status} with status transition to ${action}`,
            statusCode: 404,
            errCode: "NOT_FOUND"
        } as ErrorObject, req, res);
        return false;
    }

    return true;
}


const datasetStatusTransition = async (req: Request, res: Response) => {

    const headers = logHeaders(req, res)
    const { dataset_id, status } = _.get(req.body, "request");
    if (!validateRequest(req, res, headers)) {
        return;
    }

    const dataset:Record<string, any> = (_.includes(liveDatasetActions, status)) ? await datasetService.getDataset(dataset_id, ["id", "status", "type"], true) : await datasetService.getDraftDataset(dataset_id, ["id", "dataset_id", "status", "type"])
    
    if(!validateDataset(req, res, dataset, status, headers)) {
        return;
    }

    switch(status) {
        case "Delete":
            await deleteDataset(dataset);
            break;
        case "ReadyToPublish":
            await readyForPublish(dataset);
            break;
        case "Live":
            await publishDataset(dataset);
            break;
        case "Retire":
            await retireDataset(dataset);
            break;
        case "Archive":
            await archiveDataset(dataset);
            break;
        case "Purge":
            await purgeDataset(dataset);        
            break;
    }

    logger.info({ headers, message: `Dataset status transition to ${status} successful with id:${dataset_id}` })
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: { message: `Dataset status transition to ${status} successful`, dataset_id } });

}


// Delete a draft dataset
const deleteDataset = async (dataset: Record<string, any>) => {

    // TODO: Delete any sample files or schemas that are uploaded 
    const { id } = dataset
    const transaction = await sequelize.transaction()
    try {
        await DatasetTransformationsDraft.destroy({ where: { dataset_id: id } , transaction})
        await DatasetSourceConfigDraft.destroy({ where: { dataset_id: id } , transaction})
        await DatasourceDraft.destroy({ where: { dataset_id: id } , transaction})
        await DatasetDraft.destroy({ where: { id } , transaction})
        await transaction.commit()
    } catch (err) {
        await transaction.rollback()
        throw err
    }
}


const readyForPublish = async (dataset: Record<string, any>) => {
    
    const draftDataset: any = await datasetService.getDraftDataset(dataset.dataset_id)
    const datasetValid: Record<string, any> = schemaValidation(draftDataset, ReadyToPublishSchema)
    if (!datasetValid.isValid) {
        throw {
            code: "DATASET_CONFIGS_INVALID",
            message: datasetValid.message,
            errCode: "BAD_REQUEST",
            statusCode: 400
        }
    }
    _.set(draftDataset, 'status', DatasetStatus.ReadyToPublish)
    await DatasetDraft.update(draftDataset, { where: { id: dataset.id } })
}



//PUBLISH_DATASET
const publishDataset = async (dataset: Record<string, any>) => {

    const draftDataset: any = await datasetService.getDraftDataset(dataset.dataset_id)
    
    await validateAndUpdateDenormConfig(draftDataset);
    await updateMaterDataConfig(draftDataset)
    await DatasetDraft.update(draftDataset, { where: { id: dataset.id } })
    await generateDataSouce(draftDataset)
    
    await executeCommand(dataset.id, "PUBLISH_DATASET");
}

const validateAndUpdateDenormConfig = async (draftDataset: any) => {

    // 1. Check if there are denorm fields and dependent master datasets are published
    const denormConfig = _.get(draftDataset, "denorm_config")
    if(denormConfig && !_.isEmpty(denormConfig.denorm_fields)) {
        const datasetIds = _.map(denormConfig.denorm_fields, 'dataset_id')
        if(_.includes(datasetIds, draftDataset.id)) {
            throw {
                code: "SELF_REFERENCING_MASTER_DATA",
                message: `The denorm master dataset is self-referencing itself`,
                errCode: "SELF_REFERENCING_MASTER_DATA",
                statusCode: 409
            }
        }
        const masterDatasets = await datasetService.findDatasets({id: datasetIds, type: "master"}, ["id", "status", "dataset_config", "api_version"])
        const masterDatasetsStatus = _.map(denormConfig.denorm_fields, (denormField) => {
            const md = _.find(masterDatasets, (master) => { return denormField.dataset_id === master.id })
            let datasetStatus : Record<string, any> = {
                dataset_id: denormField.dataset_id,
                exists: (md) ? true : false,
                isLive:  (md) ? md.status === "Live" : false,
                status: md.status
            }
            if(md.api_version === "v2")
                datasetStatus['denorm_field'] = _.merge(denormField, {redis_db: md.dataset_config.cache_config.redis_db});
            else 
                datasetStatus['denorm_field'] = _.merge(denormField, {redis_db: md.dataset_config.redis_db});

            return datasetStatus;
        })
        const invalidMasters = _.filter(masterDatasetsStatus, {isLive: false})
        if(_.size(invalidMasters) > 0) {
            const invalidIds = _.map(invalidMasters, 'dataset_id')
            throw {
                code: "DEPENDENT_MASTER_DATA_NOT_LIVE",
                message: `The datasets with id:${invalidIds} are not in published status`,
                errCode: "DEPENDENT_MASTER_DATA_NOT_LIVE",
                statusCode: 428
            }
        }

        // 2. Populate redis db for denorm
        draftDataset["denorm_config"] = {
            redis_db_host: defaultDatasetConfig.denorm_config.redis_db_host,
            redis_db_port: defaultDatasetConfig.denorm_config.redis_db_port,
            denorm_fields: _.map(masterDatasetsStatus, 'denorm_field')
        }
    }
}

const updateMaterDataConfig = async (draftDataset: any) => {
    if(draftDataset.type === 'master') {
        if(draftDataset.dataset_config.cache_config.redis_db === 0) {
            const { results }: any = await query("SELECT nextval('redis_db_index')")
            if(_.isEmpty(results)) {
                throw {
                    code: "REDIS_DB_INDEX_FETCH_FAILED",
                    message: `Unable to fetch the redis db index for the master data`,
                    errCode: "REDIS_DB_INDEX_FETCH_FAILED",
                    statusCode: 500
                }
            }
            const nextRedisDB = parseInt(_.get(results, "[0].nextval")) || 3;
            _.set(draftDataset, 'dataset_config.cache_config.redis_db', nextRedisDB)
        }
    }
}

const generateDataSouce = async (draftDataset: any) => {

}

const retireDataset = async (dataset: Record<string, any>) => {

    await canRetireIfMasterDataset(dataset);
    await updateDatasetStatusToRetired(dataset);
    await deleteDruidSupervisors(dataset);
    await restartPipeline(dataset);
}


const canRetireIfMasterDataset = async (dataset: Record<string, any>) => {

    if (dataset.type === DatasetType.master) {

        const liveDatasets = await Dataset.findAll({ where: { status: DatasetStatus.Live}, attributes: ["denorm_config", "id", "status"], raw: true }) || []
        const draftDatasets = await DatasetDraft.findAll({ where: { status: [DatasetStatus.ReadyToPublish, DatasetStatus.Draft] }, attributes: ["denorm_config", "id", "status"], raw: true }) || []
        const allDatasets = _.union(liveDatasets, draftDatasets)
        const extractDenormFields = _.map(allDatasets, function(depDataset) {
            return {dataset_id: _.get(depDataset, 'id'), status: _.get(depDataset, 'status'), denorm_datasets: _.map(_.get(depDataset, 'denorm_config.denorm_fields'), 'dataset_id')}
        })
        const deps = _.filter(extractDenormFields, function(depDS) { return _.includes(depDS.denorm_datasets, dataset.id)})
        if (_.size(deps) > 0) {
            const denormErrMsg = `Failed to retire dataset as it is in use. Please retire or delete dependent datasets before retiring this dataset`
            logger.error(denormErrMsg);
            throw {
                code: "DATASET_IN_USE",
                errCode: "BAD_REQUEST",
                message: denormErrMsg,
                statusCode: 400,
                data: _.map(deps, function(o) { return _.omit(o, 'denorm_datasets')})
            }
        }
    }
}

const updateDatasetStatusToRetired = async (dataset: Record<string, any>) => {

    const transaction = await sequelize.transaction()
    try {
        await Dataset.update({ status: DatasetStatus.Retired }, { where: { id: dataset.id }, transaction })
        await DatasetSourceConfig.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id }, transaction })
        await Datasource.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id } , transaction})
        await DatasetTransformations.update({ status: DatasetStatus.Retired }, { where: { dataset_id: dataset.id } , transaction})
        await transaction.commit()
    } catch(err:any) {
        await transaction.rollback()
        throw {
            code: "UNABLE_TO_RETIRE_DATASET",
            errCode: "SERVER_ERROR",
            message: err.message,
            statusCode: 500
        }
    }
}

const deleteDruidSupervisors = async (dataset: Record<string, any>) => {

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

//RESTART_PIPELINE
const restartPipeline = async (dataset: Record<string, any>) => {
    return executeCommand(dataset.id, "RESTART_PIPELINE")
}

const archiveDataset = async (dataset: Record<string, any>) => {

    throw {
        code: "ARCHIVE_NOT_IMPLEMENTED",
        errCode: "NOT_IMPLEMENTED",
        message: "Archive functionality is not implemented",
        statusCode: 501
    }
}

const purgeDataset = async (dataset: Record<string, any>) => {

    throw {
        code: "PURGE_NOT_IMPLEMENTED",
        errCode: "NOT_IMPLEMENTED",
        message: "Purge functionality is not implemented",
        statusCode: 501
    }
}

export default datasetStatusTransition;