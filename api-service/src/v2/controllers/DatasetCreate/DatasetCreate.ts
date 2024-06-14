import { Request, Response } from "express";
import logger from "../../logger";
import { generateDataSource, getDefaultValue, getDraftDataset, getDuplicateConfigs, getDuplicateDenormKey, setReqDatasetId } from "../../services/DatasetService";
import _ from "lodash";
import DatasetCreate from "./DatasetCreateValidationSchema.json";
import { schemaValidation } from "../../services/ValidationService";
import { DatasetDraft } from "../../models/DatasetDraft";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import { ErrorObject } from "../../types/ResponseModel";
import { DatasourceDraft } from "../../models/DatasourceDraft";
import { DatasetTransformationsDraft } from "../../models/TransformationDraft";

export const apiId = "api.datasets.create"

const datasetCreate = async (req: Request, res: Response) => {
    const requestBody = req.body
    const msgid = _.get(req, ["body", "params", "msgid"]);
    const resmsgid = _.get(res, "resmsgid");
    try {
        const datasetId = _.get(req, ["body", "request", "dataset_id"])
        setReqDatasetId(req, datasetId)

        const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetCreate)

        if (!isRequestValid.isValid) {
            const code = "DATASET_INVALID_INPUT"
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: isRequestValid.message })
            return ResponseHandler.errorResponse({
                code,
                message: isRequestValid.message,
                statusCode: 400,
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }

        const datasetBody = req.body.request;
        const isDataSetExists = await checkDatasetExists(_.get(datasetBody, ["dataset_id"]));
        if (isDataSetExists) {
            const code = "DATASET_EXISTS"
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: `Dataset Already exists with id:${_.get(datasetBody, "dataset_id")}` })
            return ResponseHandler.errorResponse({
                code,
                message: "Dataset already exists",
                statusCode: 409,
                errCode: "CONFLICT"
            } as ErrorObject, req, res);
        }

        const duplicateDenormKeys = getDuplicateDenormKey(_.get(datasetBody, "denorm_config"))
        if (!_.isEmpty(duplicateDenormKeys)) {
            const code = "DATASET_DUPLICATE_DENORM_KEY"
            logger.error({ code, apiId, msgid, requestBody, resmsgid, message: `Duplicate denorm output fields found. Duplicate Denorm out fields are [${duplicateDenormKeys}]` })
            return ResponseHandler.errorResponse({
                code,
                statusCode: 400,
                message: "Duplicate denorm key found",
                errCode: "BAD_REQUEST"
            } as ErrorObject, req, res);
        }

        const datasetPayload: any = await getDefaultValue(datasetBody);
        const data = { ...datasetPayload, version_key: Date.now().toString() }

        const response = await DatasetDraft.create(data)

        const { dataset_config, denorm_config, transformation_config, data_schema, id, dataset_id } = data
        const datasourcePayload = await generateDataSource({ indexCol: _.get(dataset_config, ["timestamp_key"]), data_schema, id, dataset_id, denorm_config, transformation_config, action:"create" })
        await DatasourceDraft.create(datasourcePayload)
        logger.info({ apiId, message: `Datasource created successsfully for the dataset:${id}` })

        const transformationConfig: any = getTransformationConfig({ transformationPayload: _.get(datasetBody, "transformations_config"), datasetId: _.get(datasetPayload, "id") })
        if (!_.isEmpty(transformationConfig)) {
            await DatasetTransformationsDraft.bulkCreate(transformationConfig);
            logger.info({ apiId, message: `Dataset transformations records created successsfully for dataset:${id}` })
        }
        
        const responseData = { id: _.get(response, ["dataValues", "id"]) || "", version_key: data.version_key }
        logger.info({ apiId, msgid, requestBody, resmsgid, message: `Dataset Created Successfully with id:${_.get(response, ["dataValues", "id"])}`, response: responseData })
        ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: responseData });
    } catch (error: any) {
        const code = _.get(error, "code") || "DATASET_CREATION_FAILURE"
        logger.error({ ...error, apiId, code, msgid, requestBody, resmsgid })
        let errorMessage = error;
        const statusCode = _.get(error, "statusCode")
        if (!statusCode || statusCode == 500) {
            errorMessage = { code, message: "Failed to create dataset" }
        }
        ResponseHandler.errorResponse(errorMessage, req, res);
    }
}

const checkDatasetExists = async (dataset_id: string): Promise<boolean> => {
    const datasetExists = await getDraftDataset(dataset_id)
    if (datasetExists) {
        return true;
    } else {
        return false
    }
}

const getTransformationConfig = (configs: Record<string, any>): Record<string, any> => {
    const { transformationPayload, datasetId } = configs
    if (transformationPayload) {

        let transformations: any = []
        const transformationFieldKeys = _.flatten(_.map(transformationPayload, fields => _.get(fields, ["field_key"])))
        const duplicateFieldKeys: Array<string> = getDuplicateConfigs(transformationFieldKeys)

        if (!_.isEmpty(duplicateFieldKeys)) {
            logger.info({ apiId, message: `Duplicate transformations provided by user are [${duplicateFieldKeys}]` })
        }

        _.forEach(transformationPayload, payload => {
            const fieldKey = _.get(payload, "field_key")
            const transformationExists = _.some(transformations, field => _.get(field, "field_key") == fieldKey)
            if (!transformationExists) {
                transformations = _.flatten(_.concat(transformations, { ...payload, id: `${datasetId}_${fieldKey}`, dataset_id: datasetId }))
            }
        })
        return transformations
    }
    return []
}

export default datasetCreate;