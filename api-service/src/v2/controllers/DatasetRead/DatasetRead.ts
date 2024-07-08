import { Request, Response } from "express";
import httpStatus from "http-status";
import _ from "lodash";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { ErrorObject } from "../../types/ResponseModel";
import { DatasetDraft } from "../../models/DatasetDraft";
import { datasetService } from "../../services/DatasetService";

export const apiId = "api.datasets.read";
export const errorCode = "DATASET_READ_FAILURE"

// TODO: Move this to a config
const defaultFields = ["dataset_id", "name", "type", "status", "tags", "version", "api_version", "dataset_config"]

const isValidRequest = (req: Request, res: Response): boolean => {
    const { dataset_id } = req.params;
    const fields = req.query.fields;
    if(fields && typeof fields !== 'string') {
        logger.error({ code: "DATASET_INVALID_FIELDS_VAL", apiId, dataset_id, message: `The specified fields [${fields}] in the query param is not a string.` })
        ResponseHandler.errorResponse({
            code: "DATASET_INVALID_FIELDS_VAL",
            message: `The specified fields [${fields}] in the query param is not a string.`,
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }
    const fieldValues = fields ? _.split(fields, ",") : []
    const invalidFields = _.difference(fieldValues, Object.keys(DatasetDraft.getAttributes()))
    if (!_.isEmpty(invalidFields)) {
        logger.error({ code: "DATASET_INVALID_FIELDS", apiId, dataset_id, message: `The specified fields [${invalidFields}] in the dataset cannot be found` })
        ResponseHandler.errorResponse({
            code: "DATASET_INVALID_FIELDS",
            message: `The specified fields [${invalidFields}] in the dataset cannot be found.`,
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject, req, res);
        return false;
    }

    return true;
}

const datasetRead = async (req: Request, res: Response) => {

    if(!isValidRequest(req, res)) {
        return;
    }
    const { dataset_id } = req.params;
    const { fields, mode } = req.query;
    const attributes = !fields ? defaultFields : _.split(<string>fields, ",");
    const dataset = (mode == "edit") ? await readDraftDataset(dataset_id, attributes) : await readDataset(dataset_id, attributes)
    if(!dataset) {
        logger.error({ code: "DATASET_NOT_FOUND", apiId, dataset_id, message: `Dataset with the given dataset_id:${dataset_id} not found` })
        ResponseHandler.errorResponse({
            code: "DATASET_NOT_FOUND",
            message: "Dataset with the given dataset_id not found",
            statusCode: 404,
            errCode: "NOT_FOUND"
        } as ErrorObject, req, res);
    } else {
        ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: dataset });
    }
}

const readDraftDataset = async (datasetId: string, attributes: string[]): Promise<any> => {
    
    const attrs = _.union(attributes, ["dataset_config", "api_version", "type"])
    const draftDataset = await datasetService.getDraftDataset(datasetId, attrs);
    if(draftDataset) { // Contains a draft
        const apiVersion = _.get(draftDataset, ["api_version"]);
        const dataset: any = (apiVersion === "v2") ? draftDataset : await datasetService.migrateDraftDataset(datasetId, draftDataset)
        return _.pick(dataset, attributes); 
    }

    const liveDataset = await datasetService.getDataset(datasetId, undefined, true);
    if(liveDataset) {
        const dataset = await datasetService.createDraftDatasetFromLive(liveDataset)
        return _.pick(dataset, attributes); 
    }
    
    return null;
}

const readDataset = async (datasetId: string, attributes: string[]): Promise<any> => {
    const dataset = await datasetService.getDataset(datasetId, attributes, true);
    return dataset;
}

export default datasetRead;