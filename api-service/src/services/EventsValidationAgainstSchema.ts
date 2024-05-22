import { Request, Response, NextFunction } from "express";
import * as _ from "lodash"
import { schemaValidation } from "../helpers/ValidationService";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { dbConnector } from "../routes/Router";
import { ErrorResponseHandler } from "../helpers/ErrorResponseHandler";
import { config } from "../configs/Config";

export const eventsValidationAgainstSchema = async (req: Request, res: Response, next: NextFunction) => {
    const errorHandler = new ErrorResponseHandler("DatasetService");
    try {
        const isLive = _.get(req, "body.isLive");
        const event = _.get(req, "body.event");
        const filters = _.get(req, "body.filters")
        let datasetRecord = await dbConnector.readRecords(isLive ? config.table_names.datasets : `${config.table_names.datasets}_draft`, { filters: filters })
        let schema = _.get(datasetRecord, "[0].data_schema")

        if (_.isEmpty(datasetRecord)) {
            throw {
                "message": `Dataset ${filters?.dataset_id} does not exists`,
                "status": 404,
                "code": "NOT_FOUND"
            }
        }

        const validateEventAgainstSchema = schemaValidation(event, _.omit(schema, "$schema"));
        ResponseHandler.successResponse(req, res, { status: 200, data: { message: validateEventAgainstSchema?.message } });
    }
    catch (error) {
        return errorHandler.handleError(req, res, next, error);
    }
}