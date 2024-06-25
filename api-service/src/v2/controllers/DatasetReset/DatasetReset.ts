import { Request, Response } from "express";
import _ from "lodash";
import { schemaValidation } from "../../services/ValidationService";
import DatasetResetRequestSchema from "./DatasetResetValidationSchema.json"
import { ErrorObject } from "../../types/ResponseModel";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { DatasetStatus, HealthStatus } from "../../types/DatasetModels";
import { Dataset } from "../../models/Dataset";
import logger from "../../logger";
import {  getInfraHealth,  getProcessingHealth, getQueryHealth } from "../../services/HealthService";
import { Datasource } from "../../models/Datasource";

export const apiId = "api.dataset.reset";
export const errorCode = "DATASET_RESET_FAILURE"



const datasetReset = async (req: Request, res: Response) => {
    const resmsgid = _.get(res, "resmsgid");
    const requestBody = req.body;
    const datasetId = _.get(req, "params.datasetId")
    const msgid = _.get(req, ["body", "params", "msgid"]);
    try {
        const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetResetRequestSchema)
        if (!isRequestValid.isValid) {
            const code = "DATASET_RESET_INPUT_INVALID"
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: isRequestValid.message })
            return ResponseHandler.errorResponse({
                code,
                message: isRequestValid.message,
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }

        const dataset = await getLiveDatasets(datasetId)
        if(_.isEmpty(dataset)) {
            const code = "DATASET_RESET_NO_DATASET"
            const message = `There are no live dataset exists with given dataset_id: ${datasetId}`
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: message })
            return ResponseHandler.errorResponse({
                code,
                message,
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }
        logger.debug(apiId, msgid, resmsgid, "dataset", dataset)


    } catch (error: any) {
        logger.error({ ...error, apiId, code: errorCode, msgid, requestBody, resmsgid });
        let errorMessage = error;
        const statusCode = _.get(error, "statusCode")
        if (!statusCode || statusCode == 500) {
            errorMessage = { code: errorCode, message: "Failed to reset the dataset" }
        }
        ResponseHandler.errorResponse(errorMessage, req, res);
    }



}
const getLiveDatasets = async (ids: string): Promise<Record<string, any>> => {
    return Dataset.findAll({ attributes: ['dataset_id', 'status', 'type'], where: { dataset_id: ids, status: DatasetStatus.Live }, raw: true });
}

const getDataSources = async (ids: Record<string, any>): Promise<Record<string, any>> => {
    return Datasource.findAll({ attributes: ['dataset_id', 'datasource'], where: { dataset_id: ids }, raw: true });
}

export default datasetReset;