import { Request, Response } from "express";
import _ from "lodash";
import { executeNativeQuery } from "../../connections/druidConnection";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import vaidationSchema from "./nativeQueryValidationSchema.json"
import { schemaValidation } from "../../services/ValidationService";
import logger from "../../logger";
import { obsrvError } from "../../types/ObsrvError";

const nativeQuery = async (req: Request, res: Response) => {
    const isValidSchema = schemaValidation(req.body, vaidationSchema);
    if (!isValidSchema?.isValid) {
        logger.error({ message: isValidSchema?.message, code: "INVALID_QUERY" })
        throw obsrvError("", "INVALID_QUERY", isValidSchema.message, "BAD_REQUEST", 400)
    }
    const query = _.get(req, ["body", "query", "query"]);
    const response = await executeNativeQuery(query);
    ResponseHandler.successResponse(req, res, { status: 200, data: _.get(response, "data") });
}

export default nativeQuery;