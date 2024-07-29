import Ajv from "ajv";
import _ from "lodash";
import { obsrvError } from "../../types/ObsrvError";
import ValidationSchema from "./RequestValidationSchema.json"
import { defaultDatasetConfig } from "../../configs/DatasetConfigDefault";
import { schemaValidation } from "../../services/ValidationService";
import { DatasetStatus, DatasetType } from "../../types/DatasetModels";
import { datasetService } from "../../services/DatasetService";
const validator = new Ajv();

const reqBodySchema = ValidationSchema.request_body
const transformationSchema = ValidationSchema.transformations_config
const connectorSchema = ValidationSchema.connectors_config
const denormSchema = ValidationSchema.denorm_config

const validateConfigs = (schema: any, configs: any[]): { valid: any[], ignored: any[] } => {
    const validConfigs: any[] = [];
    const ignoredConfigs: any[] = [];

    for (const config of configs) {
        if (validator.validate(schema, config)) {
            validConfigs.push(config);
        } else {
            const error: any = validator.errors;
            const errorMessage = error[0]?.schemaPath?.replace("/", "") + " " + error[0]?.message || "Invalid Request Body";
            ignoredConfigs.push({ config, details: errorMessage });
        }
    }

    return { valid: validConfigs, ignored: ignoredConfigs };
};

export const datasetImportValidation = async (payload: Record<string, any>): Promise<Record<string, any>> => {
    const isRequestValid: Record<string, any> = schemaValidation(payload, reqBodySchema)
    if (!isRequestValid.isValid) {
        throw obsrvError("", "DATASET_IMPORT_INVALID_CONFIGS", isRequestValid.message, "BAD_REQUEST", 400)
    }

    let datasetConfig = payload.request;

    const connectors = _.get(datasetConfig, "connectors_config", []);
    const transformations = _.get(datasetConfig, "transformations_config", []);
    const denormConfig = _.get(datasetConfig, "denorm_config", { denorm_fields: [] });
    const { validDenorms, invalidDenorms } = await validateDenorms(denormConfig)

    const { valid: resultantConnectors, ignored: ignoredConnectors } = validateConfigs(connectorSchema, connectors);
    const { valid: resultantTransformations, ignored: ignoredTransformations } = validateConfigs(transformationSchema, transformations);
    const { valid: resultantDenorms, ignored: ignoredDenorms } = validateConfigs(denormSchema, validDenorms);

    datasetConfig["connectors_config"] = resultantConnectors;
    datasetConfig["transformations_config"] = resultantTransformations;
    datasetConfig["denorm_config"] = { ...denormConfig, denorm_fields: resultantDenorms };
    datasetConfig["router_config"] = { topic: datasetConfig.id }
    datasetConfig["version_key"] = Date.now().toString()

    const defaults = _.cloneDeep(defaultDatasetConfig);
    const resultantDataset = _.merge(defaults, datasetConfig);

    return {
        updatedDataset: _.omit(resultantDataset, ["created_date", "updated_date", "published_date", "status"]),
        ignoredFields: { ignoredConnectors, ignoredTransformations, ignoredDenorms: [...ignoredDenorms, ...invalidDenorms] }
    };
};

const validateDenorms = async (denormConfig: Record<string, any>): Promise<Record<string, any>> => {
    const invalidDenorms: any[] = [];
    const validDenorms: any[] = [];

    if (denormConfig && !_.isEmpty(denormConfig.denorm_fields)) {
        const masterDatasets = await datasetService.findDatasets({ status: DatasetStatus.Live, type: DatasetType.master }, ["id", "dataset_id", "status", "dataset_config", "api_version"]);

        for (const field of denormConfig.denorm_fields) {
            const { redis_db, dataset_id, denorm_out_field, denorm_key } = field;
            let masterDataset;

            if (dataset_id) {
                masterDataset = _.find(masterDatasets, dataset => _.get(dataset, "dataset_id") === dataset_id);
            } else if (redis_db) {
                masterDataset = _.find(masterDatasets, dataset => _.get(dataset, "dataset_config.cache_config.redis_db") === redis_db);
            }

            const denormFields = { denorm_key, denorm_out_field, dataset_id: dataset_id || _.get(masterDataset, "dataset_id") }
            if (masterDataset) {
                validDenorms.push(denormFields);
            } else {
                invalidDenorms.push({ config: denormFields, details: `Master dataset does not exist` });
            }
        }
    }

    return { validDenorms, invalidDenorms };
};

export const migrateExportedDatasetV1 = (datasetPayload: Record<string, any>) => {

    const { dataset_id, timestamp_key = "", data_key = "", type: datasetType } = _.get(datasetPayload, "data.metadata")
    const type = datasetType === "master-dataset" ? DatasetType.master : DatasetType.event

    let dataset: Record<string, any> = {
        dataset_id, id: dataset_id, name: dataset_id, type,
        version_key: Date.now().toString(),
        api_version: "v2",
    };

    const { validation, dedup, batch } = _.get(datasetPayload, "data.config")
    dataset["data_schema"] = _.get(datasetPayload, "data.data_schema")
    dataset["dedup_config"] = { ..._.omit(dedup, "enabled"), drop_duplicates: _.get(dedup, "enabled") };
    dataset["router_config"] = { topic: dataset_id };
    dataset["validation_config"] = { ..._.omit(validation, "enabled"), validate: _.get(validation, "enabled") };

    const { drop_duplicates, dedup_key, dedup_period, extraction_key, enabled: is_batch_event } = batch
    dataset["extraction_config"] = { is_batch_event, extraction_key, dedup_config: { drop_duplicates, dedup_key, dedup_period } }

    const { redis_db, redis_db_host, redis_db_port } = defaultDatasetConfig.dataset_config.cache_config;
    dataset["dataset_config"] = {
        indexing_config: { olap_store_enabled: true, lakehouse_enabled: false, cache_enabled: (type === DatasetType.master) },
        keys_config: { data_key, timestamp_key },
        cache_config: { redis_db_host, redis_db_port, redis_db }
    }

    dataset["denorm_config"] = {
        denorm_fields: _.map(_.get(datasetPayload, "data.denorm"), configs => {
            const { master_dataset_id, denorm_key, out } = configs;
            return { denorm_key, denorm_out_field: out, dataset_id: master_dataset_id }
        })
    }

    dataset["transformations_config"] = _.map(_.get(datasetPayload, "data.transformations", []), (config) => {
        const { type, key, expr, mode, dataType = "string", section } = config
        return {
            field_key: key,
            transformation_function: {
                type, expr,
                datatype: dataType,
                category: datasetService.getTransformationCategory(section)
            }, mode
        }
    })

    dataset["connectors_config"] = _.map(_.get(datasetPayload, "env_variables.input_sources", []), (config) => {
        const { id, type, ...rest } = config
        return {
            id, connector_id: type,
            connector_config: rest,
            version: "v1"
        }
    })

    return dataset;
}