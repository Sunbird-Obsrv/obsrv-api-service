import _ from "lodash";
import { Request, Response } from "express";
import httpStatus from "http-status";
import logger from "../../logger";
import { datasetService } from "../../services/DatasetService";
import DatasetCreate from "./DatasetCreateValidationSchema.json";
import { schemaValidation } from "../../services/ValidationService";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { ErrorObject } from "../../types/ResponseModel";

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
    const dataset = await datasetService.createDataset(req.body.request);
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: dataset });
}

export default datasetCreate;