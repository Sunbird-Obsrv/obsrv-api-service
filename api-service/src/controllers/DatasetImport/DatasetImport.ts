import { Request, Response } from "express";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import _ from "lodash";
import { datasetService } from "../../services/DatasetService";
import { datasetImportValidation, migrateExportedDatasetV1 } from "./DatasetImportHelper";
import { obsrvError } from "../../types/ObsrvError";

const datasetImport = async (req: Request, res: Response) => {

    const requestBody = req.body
    let datasetPayload = requestBody.request;
    if (_.get(datasetPayload, "api_version") !== "v2") {
        const migratedConfigs = migrateExportedDatasetV1(datasetPayload)
        datasetPayload = migratedConfigs;
    }
    const dataset_id = _.get(datasetPayload, "dataset_id")
    const { updatedDataset, ignoredFields } =await datasetImportValidation({ ...requestBody, "request": datasetPayload })
    const { successMsg, partialIgnored } = getResponseData(ignoredFields)
    const dataset = await datasetService.createDraftDataset(updatedDataset).catch(err => {
        if (err?.name === 'SequelizeUniqueConstraintError') {
            throw obsrvError(dataset_id, "DATASET_ALREADY_EXISTS", `Dataset with id ${dataset_id} already exists to import`, "BAD_REQUEST", 400);
        }
        throw obsrvError("", "DATASET_IMPORT_FAILURE", `Failed to import dataset`, "INTERNAL_SERVER_ERROR", 500);
    })
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: { message: successMsg, data: dataset, ...(!_.isEmpty(partialIgnored) && { ignoredFields: partialIgnored }) } });
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