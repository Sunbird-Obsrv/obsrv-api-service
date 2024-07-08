import { Request, Response } from "express";
import httpStatus from "http-status";
import logger from "../../logger";
import _ from "lodash";
import Model from "sequelize/types/model";
import { ErrorObject } from "../../types/ResponseModel";
import { DatasetStatus } from "../../types/DatasetModels";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { cipherService } from "../../services/CipherService";
import { datasetService } from "../../services/DatasetService";
import { schemaValidation } from "../../services/ValidationService";
import DatasetUpdate from "./DatasetUpdateValidationSchema.json";

export const apiId = "api.datasets.update";
export const invalidInputErrCode = "DATASET_UPDATE_INPUT_INVALID"
export const errorCode = "DATASET_UPDATE_FAILURE"

const isValidRequest = async (req: Request, res: Response): Promise<boolean> => {

    const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetUpdate)
    if (!isRequestValid.isValid) {
        logger.error({ code: "DATASET_UPDATE_INPUT_INVALID", apiId, body: req.body, message: isRequestValid.message })
        ResponseHandler.errorResponse({
            code: "DATASET_UPDATE_INPUT_INVALID",
            message: isRequestValid.message,
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }

    const datasetBody = req.body.request
    const { dataset_id, version_key, ...rest } = datasetBody
    if (_.isEmpty(rest)) {
        const code = "DATASET_UPDATE_NO_FIELDS"
        logger.error({ code, apiId, body: req.body, message: `Provide atleast one field in addition to the dataset_id:${dataset_id} and version_key:${version_key} to update the dataset` })
        ResponseHandler.errorResponse({
            code,
            message: "Provide atleast one field in addition to the dataset_id to update the dataset",
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }

    return true;
}

const isValidDataset = (dataset: Record<string, any> | null, req: Request, res: Response): boolean => {

    const datasetId = _.get(req, ["body", "request", "dataset_id"])
    const versionKey = _.get(req, ["body", "request", "version_key"])
    if (dataset) {
        if (dataset.api_version !== "v2") {
            logger.error({ code: "DATASET_API_VERSION_MISMATCH", apiId, body: req.body, message: `Draft dataset api version is not v2:${datasetId}` })
            ResponseHandler.errorResponse({
                code: "DATASET_API_VERSION_MISMATCH",
                message: "Draft dataset api version is not v2",
                statusCode: 404,
                errCode: "NOT_FOUND"
            } as ErrorObject, req, res);
            return false;
        } 
        if(dataset.version_key !== versionKey) {
            logger.error({ code: "DATASET_OUTDATED", body: req.body, message: `The dataset:${datasetId} with version_key:${versionKey} is outdated. Please try to fetch latest changes of the dataset with version key:${dataset.version_key} and perform the updates` })
            ResponseHandler.errorResponse({
                code: "DATASET_OUTDATED",
                message: "The dataset is outdated. Please try to fetch latest changes of the dataset and perform the updates",
                statusCode: 409,
                errCode: "CONFLICT"
            } as ErrorObject, req, res);
            return false;
        }
        if(!_.includes([DatasetStatus.Draft, DatasetStatus.ReadyToPublish], dataset.status)) {
            logger.error({ code: "DATASET_NOT_IN_DRAFT_STATE_TO_UPDATE", body: req.body, message: `Dataset with id:${datasetId} cannot be updated as it is not in draft state` })
            ResponseHandler.errorResponse({
                code: "DATASET_NOT_IN_DRAFT_STATE_TO_UPDATE",
                message: "Dataset cannot be updated as it is not in draft state",
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
            return false;
        }
        return true;
    } else {
        logger.error({ code: "DATASET_NOT_EXISTS", body: req.body, message: `Dataset does not exists with id:${datasetId}` })
        ResponseHandler.errorResponse({
            code: "DATASET_NOT_EXISTS",
            message: "Dataset does not exists to update",
            statusCode: 404,
            errCode: "NOT_FOUND"
        } as ErrorObject, req, res);
        return false;
    }
}

const datasetUpdate = async (req: Request, res: Response) => {
    
    const isRequestValid = await isValidRequest(req, res)
    if(!isRequestValid) {
        return;
    }

    const datasetReq = req.body.request;
    const datasetModel = await datasetService.getDraftDataset(datasetReq.dataset_id)
    if(!isValidDataset(datasetModel, req, res)) {
        return;
    }
    
    const draftDataset = mergeDraftDataset(datasetModel, datasetReq);
    const response = await datasetService.updateDraftDataset(draftDataset);
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: response });
}

const mergeDraftDataset = (datasetModel: Model<any, any> | null, datasetReq: any): Record<string, any> => {

    let dataset: Record<string, any> = {
        version_key: Date.now().toString(),
        name: datasetReq.name || _.get(datasetModel, ["name"]),
        id: _.get(datasetModel, ["id"])
    }
    if(datasetReq.validation_config) dataset["validation_config"] = datasetReq.validation_config
    if(datasetReq.extraction_config) dataset["extraction_config"] = datasetReq.extraction_config
    if(datasetReq.dedup_config) dataset["dedup_config"] = datasetReq.dedup_config
    if(datasetReq.data_schema) dataset["data_schema"] = datasetReq.data_schema
    if(datasetReq.dataset_config) dataset["dataset_config"] = datasetReq.dataset_config
    if(datasetReq.transformations_config) 
        dataset["transformations_config"] = mergeTransformationsConfig(_.get(datasetModel, ["transformations_config"]), datasetReq.transformations_config)
    if(datasetReq.denorm_config) dataset["denorm_config"] = mergeDenormConfig(_.get(datasetModel, ["denorm_config"]), datasetReq.denorm_config)
    if(datasetReq.connectors_config) dataset["connectors_config"] = mergeConnectorsConfig(_.get(datasetModel, ["connectors_config"]), datasetReq.connectors_config)
    if(datasetReq.tags) dataset["tags"] = mergeTags(_.get(datasetModel, ["tags"]), datasetReq.tags)
    if(datasetReq.sample_data) dataset["sample_data"] = datasetReq.sample_data

    return dataset;
}

const mergeTransformationsConfig = (currentConfigs: any, newConfigs: any) => {
    const removeConfigs = _.map(_.filter(newConfigs, {action: "remove"}), "value.field_key")
    const addConfigs = _.map(_.filter(newConfigs, {action: "upsert"}), "value")

    return _.unionWith(
        addConfigs,
        _.reject(currentConfigs, (config) => { return _.includes(removeConfigs, config.field_key)}),
        (a, b) => { 
            return a.field_key === b.field_key
        }  
    )
}

const mergeDenormConfig = (currentConfig: any, newConfig: any) => {

    const removeConfigs = _.map(_.filter(newConfig.denorm_fields, {action: "remove"}), "value.denorm_out_field")
    const addConfigs = _.map(_.filter(newConfig.denorm_fields, {action: "upsert"}), "value")

    const denormFields = _.unionWith(
        addConfigs,
        _.reject(currentConfig.denorm_fields, (config) => { return _.includes(removeConfigs, config.denorm_out_field)}),
        (a, b) => { 
            return a.denorm_out_field === b.denorm_out_field
        }  
    )
    return {
        denorm_fields: denormFields
    }
}

const mergeConnectorsConfig = (currConfigs: any, newConfigs: any) => {

    const removeConfigs = _.map(_.filter(newConfigs, {action: "remove"}), "value.connector_id")
    const addConfigs = _.map(_.filter(newConfigs, {action: "upsert"}), "value")

    return _.unionWith(
        _.map(addConfigs, (config) => {
            return {
                connector_id: config.connector_id,
                connector_config: cipherService.encrypt(JSON.stringify(config.connector_config)),
                operations_config: config.operations_config
            }
        }),
        _.reject(currConfigs, (config) => { return _.includes(removeConfigs, config.connector_id)}),
        (a, b) => { 
            return a.connector_id === b.connector_id
        }  
    )
}

const mergeTags = (currentTags: any, newConfigs: any) => {

    const tagsToRemove = _.map(_.filter(newConfigs, {action: "remove"}), "value")
    const tagsToAdd = _.map(_.filter(newConfigs, {action: "upsert"}), "value")
    return _.union(_.pullAll(currentTags, tagsToRemove), tagsToAdd)
}

export default datasetUpdate;