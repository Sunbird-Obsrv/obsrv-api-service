import { NextFunction, Request, Response } from "express";
import logger from "../logger";
import { ResponseHandler } from "../helpers/ResponseHandler";
import _ from "lodash";

export const errorHandler = (err: Error, req: Request, res: Response, next: NextFunction) => {

    logger.error({ path: req.url, req: req.body , ...err })
    let errorMessage = {name: err.name, message: err.message};
    ResponseHandler.errorResponse(errorMessage, req, res);
};