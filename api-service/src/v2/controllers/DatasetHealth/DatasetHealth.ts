import { Request, Response, response } from "express";
import _ from "lodash";
import { schemaValidation } from "../../services/ValidationService";
import DatasetHealthRequestSchema from "./DatasetHealthValidationSchema.json"
import { ErrorObject } from "../../types/ResponseModel";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { DatasetStatus, HealthStatus } from "../../types/DatasetModels";
import { Dataset } from "../../models/Dataset";
import logger from "../../logger";
import {  getInfraHealth,  getProcessingHealth } from "../../services/HealthService";

export const apiId = "api.dataset.health";
export const errorCode = "DATASET_HEALTH_FAILURE"



const datasetHealth = async (req: Request, res: Response) => {
    const resmsgid = _.get(res, "resmsgid");
    const requestBody = req.body;
    const msgid = _.get(req, ["body", "params", "msgid"]);
    try {
        const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetHealthRequestSchema)
        if (!isRequestValid.isValid) {
            const code = "DATASET_HEALTH_INPUT_INVALID"
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: isRequestValid.message })
            return ResponseHandler.errorResponse({
                code,
                message: isRequestValid.message,
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }
        if (requestBody?.request?.datasets === "*") {
            const {components, status} = await getInfraHealth()
            return ResponseHandler.successResponse(req, res, {
                status: 200, data: {
                    "status": status,
                    "details": [
                        {
                            "category": "infra",
                            "status": status,
                            "components": components
                        }]
                }
            });
        }

        const dataset = await getLiveDatasets(requestBody?.request?.datasets)
        if(_.isEmpty(dataset)) {
            const code = "DATASET_HEALTH_NO_DATASETS"
            const message = `There are no live datasets exists with given dataset_ids: ${requestBody?.request?.datasets}`
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: message })
            return ResponseHandler.errorResponse({
                code,
                message,
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }
        logger.debug(apiId, msgid, resmsgid, "dataset", dataset)

        const categories = _.map(requestBody?.request?.categories, (category) => _.get(category, 'category'));
        const details = []
        if(categories.includes("infra")) {
            const {components, status} = await getInfraHealth()
            details.push({
                "category": "infra",
                "status": status,
                "components": components
            })
        } 
        logger.debug({categories})
        if(categories.includes("processing")) {
            const {components, status} = await getProcessingHealth(dataset[0])
            details.push({
                "category": "processing",
                "status": status,
                "components": components
            })
        }

        const allStatus = _.includes(_.map(details, (detail) => detail?.status), HealthStatus.UnHealthy) ? HealthStatus.UnHealthy: HealthStatus.Healthy

        return ResponseHandler.successResponse(req, res, {
            status: 200, data: {
                "status": allStatus,
                "details": details
            }
        });

    } catch (error: any) {
        logger.error({ ...error, apiId, code: errorCode, msgid, requestBody, resmsgid });
        let errorMessage = error;
        const statusCode = _.get(error, "statusCode")
        if (!statusCode || statusCode == 500) {
            errorMessage = { code: errorCode, message: "Failed to check dataset health" }
        }
        ResponseHandler.errorResponse(errorMessage, req, res);
    }



}
const getLiveDatasets = async (ids: Record<string, any>): Promise<Record<string, any>> => {
    return Dataset.findAll({ attributes: ['dataset_id', 'status', 'type'], where: { dataset_id: ids, status: DatasetStatus.Live }, raw: true });
}

export default datasetHealth;