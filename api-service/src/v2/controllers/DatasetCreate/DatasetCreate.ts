import _ from "lodash";
import { Request, Response } from "express";
import httpStatus from "http-status";
import logger from "../../logger";
import { datasetService } from "../../services/DatasetService";
import DatasetCreate from "./DatasetCreateValidationSchema.json";
import { schemaValidation } from "../../services/ValidationService";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { ErrorObject } from "../../types/ResponseModel";
import { cipherService } from "../../services/CipherService";
import { defaultDatasetConfig } from "../../configs/DatasetConfigDefault";

export const apiId = "api.datasets.create"

const isValidRequest = async (req: Request, res: Response): Promise<boolean> => {

    const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetCreate)
    if (!isRequestValid.isValid) {
        logger.error({ code: "DATASET_INVALID_INPUT", apiId, body: req.body, message: isRequestValid.message })
        ResponseHandler.errorResponse({
            code: "DATASET_INVALID_INPUT",
            message: isRequestValid.message,
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }
    const datasetId = _.get(req, ["body", "request", "dataset_id"])
    const isDataSetExists = await datasetService.checkDatasetExists(datasetId);
    if (isDataSetExists) {
        logger.error({ code: "DATASET_EXISTS", apiId, body: req.body, message: `Dataset Already exists with id:${datasetId}` })
        ResponseHandler.errorResponse({
            code: "DATASET_EXISTS",
            message: "Dataset already exists",
            statusCode: 409,
            errCode: "CONFLICT"
        } as ErrorObject, req, res);
        return false;
    }

    const duplicateDenormKeys = datasetService.getDuplicateDenormKey(_.get(req, ["body", "request", "denorm_config"]))
    if (!_.isEmpty(duplicateDenormKeys)) {
        const code = "DATASET_DUPLICATE_DENORM_KEY"
        logger.error({ code: "DATASET_DUPLICATE_DENORM_KEY", apiId, body: req.body, message: `Duplicate denorm output fields found. Duplicate Denorm out fields are [${duplicateDenormKeys}]` })
        ResponseHandler.errorResponse({
            code: "DATASET_DUPLICATE_DENORM_KEY",
            statusCode: 400,
            message: "Duplicate denorm key found",
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }

    return true;
}

const datasetCreate = async (req: Request, res: Response) => {
    
    const isRequestValid = await isValidRequest(req, res)
    if(!isRequestValid) {
        return;
    }
    const draftDataset = getDraftDataset(req.body.request)
    const dataset = await datasetService.createDraftDataset(draftDataset);
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: dataset });
}

const getDraftDataset = (datasetReq: Record<string, any>): Record<string, any> => {
    const transformationsConfig:Array<Record<string, any>> = _.get(datasetReq, "transformations_config")
    const connectorsConfig:Array<Record<string, any>> = _.get(datasetReq, "connectors_config")
    const dataset = _.omit(datasetReq, ["transformations_config", "connectors_config"])
    const mergedDataset = mergeDatasetConfigs(defaultDatasetConfig, dataset)
    const draftDataset = { 
        ...mergedDataset, 
        version_key: Date.now().toString(),
        transformations_config: getDatasetTransformations(transformationsConfig),
        connectors_config: getDatasetConnectors(connectorsConfig),
    }
    return draftDataset;
}

const mergeDatasetConfigs = (defaultConfig: Record<string, any>, requestPayload: Record<string, any>): Record<string, any> => {
    const { id, dataset_id } = requestPayload;
    const datasetId = !id ? dataset_id : id
    const modifyPayload = { ...requestPayload, id: datasetId, router_config: { topic: datasetId } }
    const defaults = _.cloneDeep(defaultConfig)
    const datasetConfigs = _.merge(defaults, modifyPayload)
    return datasetConfigs
}

const getDatasetConnectors = (connectorConfigs: Array<Record<string, any>>): Array<Record<string, any>> => {
    
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

const getDatasetTransformations = (configs: Array<Record<string, any>>): Array<Record<string, any>> => {

    if (configs) {
        return _.uniqBy(configs, "field_key")
    }
    return []
}

export default datasetCreate;