import { Request, Response } from "express";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import _ from "lodash";
import { datasetService } from "../../services/DatasetService";
import { datasetImportValidation, migrateExportedDatasetV1 } from "./DatasetImportHelper";
import { obsrvError } from "../../types/ObsrvError";

const datasetImport = async (req: Request, res: Response) => {

    const { overwrite = "true" } = req.query;
    const requestBody = req.body
    let datasetPayload = requestBody.request;
    if (_.get(datasetPayload, "api_version") !== "v2") {
        const migratedConfigs = migrateExportedDatasetV1(requestBody)
        datasetPayload = migratedConfigs;
    }
    const { updatedDataset, ignoredFields } = await datasetImportValidation({ ...requestBody, "request": datasetPayload })
    const { successMsg, partialIgnored } = getResponseData(ignoredFields)

    const dataset = overwrite == "true" ? await overWriteDataset(updatedDataset) : await createNewDataset(updatedDataset)
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: { message: successMsg, data: dataset, ...(!_.isEmpty(partialIgnored) && { ignoredFields: partialIgnored }) } });
}

const createNewDataset = async (dataset: Record<string, any>) => {
    return datasetService.createDraftDataset(dataset).catch(err => {
        if (err?.name === 'SequelizeUniqueConstraintError') {
            const dataset_id = _.get(dataset, "dataset_id")
            throw obsrvError(dataset.dataset_id, "DATASET_ALREADY_EXISTS", `Dataset with id : ${dataset_id} already exists to import`, "BAD_REQUEST", 400);
        }
        throw obsrvError("", "DATASET_IMPORT_FAILURE", `Failed to import dataset`, "INTERNAL_SERVER_ERROR", 500);
    })
}

const overWriteDataset = async (dataset: Record<string, any>) => {
    const dataset_id = _.get(dataset, "dataset_id")
    const draftDataset = await datasetService.getDraftDataset(dataset_id, ["id"])
    if (!draftDataset) {
        throw obsrvError(dataset.dataset_id, "DATASET_NOT_FOUND", `Dataset with dataset_id: ${dataset_id} not found to overwrite`, "NOT_FOUND", 404)
    }
    const response = await datasetService.updateDraftDataset(dataset)
    return _.omit(response, ["message"])
}

const getResponseData = (ignoredConfigs: Record<string, any>) => {
    const { ignoredConnectors, ignoredTransformations, ignoredDenorms } = ignoredConfigs;
    let successMsg = "Dataset is imported successfully";
    let partialIgnored: Record<string, any> = {};

    if (ignoredConnectors.length || ignoredTransformations.length || ignoredDenorms.length) {
        successMsg = "Dataset is partially imported";

        if (ignoredTransformations.length) {
            partialIgnored.transformations = ignoredTransformations;
        }
        if (ignoredConnectors.length) {
            partialIgnored.connectors = ignoredConnectors;
        }
        if (ignoredDenorms.length) {
            partialIgnored.denorm_fields = ignoredDenorms;
        }
    }

    return { successMsg, partialIgnored };
}

export default datasetImport;