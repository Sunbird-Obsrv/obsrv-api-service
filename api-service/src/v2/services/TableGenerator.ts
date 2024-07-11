import _ from "lodash";
import { rawIngestionSpecDefaults } from "../configs/IngestionConfig";
import { datasetService } from "./DatasetService";

class BaseTableGenerator {

    /**
     * Method to flatten a json schema - extract all properties using jsonpath notation
     * 
     * @param dataSchema 
     * @returns properties Record<string, any>[]
     */
    flattenSchema = (dataSchema: Record<string, any>) : Record<string, any>[] => {

        let properties: Record<string, any>[] = []
        const flatten = (schema: Record<string, any>, prev: string | undefined, prevExpr: string | undefined) => {
            _.mapKeys(schema, function(value, parentKey) {
                const newKey = (prev) ? _.join([prev, parentKey], '.') : parentKey;
                const newExpr = (prevExpr) ? _.join([prevExpr, ".['", parentKey, "']"], '') : _.join(["$.['", parentKey, "']"], '');
                switch(value['type']) {
                    case 'object':     
                        flatten(_.get(value, 'properties'), newKey, newExpr);
                        break;
                    case 'array':
                        if(_.get(value, 'items.type') == 'object' && _.get(value, 'items.properties')) {
                            _.mapKeys(_.get(value, 'items.properties'), function(value, childKey) {
                                const objChildKey = _.join([newKey, childKey], '.')
                                properties.push(_.merge(_.pick(value, ['arrival_format', 'is_deleted']), {expr: _.join([newExpr,"[*].['",childKey,"']"], ''), name: objChildKey, data_type: 'array'}))
                            })
                        } else {
                            properties.push(_.merge(_.pick(value, ['arrival_format', 'data_type', 'is_deleted']), {expr: newExpr, name: newKey}))
                        }
                        break;
                    default:
                        properties.push(_.merge(_.pick(value, ['arrival_format', 'data_type', 'is_deleted']), {expr: newExpr, name: newKey}))
                }
            });
        }
        flatten(_.get(dataSchema, 'properties'), undefined, undefined)
        return properties
    }

    /**
     * Get all fields of a dataset merging schema fields, transformations and denorm fields
     * 
     * @param data_schema 
     * @param transformations_config 
     * @param denorm_config 
     * @returns Promise<Record<string, any>[]>
     */
    getAllFields = async (data_schema: any, transformations_config: any, denorm_config: any) : Promise<Record<string, any>[]> => {
        const instance = this;
        let dataFields = instance.flattenSchema(data_schema);
        if(denorm_config.denorm_fields) {
            _.map(denorm_config.denorm_fields, async (denormField) => {
                const denormDataset: any = await datasetService.getDataset(denormField.dataset_id, ["data_schema"], true);
                const properties = instance.flattenSchema(denormDataset.data_schema);
                const transformProps = _.map(properties, (prop) => {
                    _.set(prop, 'name', _.join([denormField.denorm_out_field, prop.name], '.'));
                    _.set(prop, 'expr', _.replace(prop.expr, "$", "$." + denormField.denorm_out_field));
                    return prop;
                });
                dataFields.push(...transformProps);
            })
        }
        if(transformations_config) {
            _.map(transformations_config, async (tf) => {
                dataFields.push({
                    expr: "$." + tf.field_key,
                    name: tf.field_key,
                    data_type: tf.transformation_function.datatype
                })
            })
        }
        dataFields.push(rawIngestionSpecDefaults.synctsField)
        _.remove(dataFields, {is_deleted: true}) // Delete all the excluded fields
        return dataFields;
    }
}

class TableGenerator extends BaseTableGenerator {

    getDruidIngestionSpec = async (dataset: Record<string, any>, datasourceRef: string) => {
        
        const { data_schema, denorm_config, dataset_config, router_config, transformations_config} = dataset
        const allFields = await this.getAllFields(data_schema, transformations_config, denorm_config);        
        return {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceRef,
                    "dimensionsSpec": { "dimensions": this.getDimensions(allFields, dataset_config.keys_config.timestamp_key, dataset_config.keys_config.partition_key) },
                    "timestampSpec": { "column": dataset_config.keys_config.timestamp_key, "format": "auto" },
                    "metricsSpec": [],
                    "granularitySpec": rawIngestionSpecDefaults.granularitySpec
                },
                "tuningConfig": rawIngestionSpecDefaults.tuningConfig,
                "ioConfig": _.merge(rawIngestionSpecDefaults.ioConfig, {
                    "topic": router_config.topic,
                    "inputFormat": {
                        "flattenSpec": {
                            "fields": this.getFlattenSpec(allFields)
                        }
                    }
                })
            }
        }
    }
    
    getDimensions = (allFields: Record<string, any>[], timestampKey: string, partitionKey: string | undefined) => {
        if(partitionKey) { // Move the partition column to the top of the dimensions
            const partitionCol = _.remove(allFields, {name: partitionKey})
            if(partitionCol && _.size(partitionCol) > 0) {
                allFields.unshift(partitionCol[0])
            }
        }
        const instance = this;
        return _.union(
            _.map(allFields, (field) => {
                if(field.name !== timestampKey) {
                    return {
                        "type": instance.getType(field.data_type),
                        "name": field.name
                    }
                }
            }),
            rawIngestionSpecDefaults.dimensions
        )
    }

    getType = (data_type: string):string => {
        switch (data_type) {
            case "number": return "double";
            case "integer": return "long";
            case "object": return "json";
            case "boolean": return "string";
            case "array": return "json";
            case "string": return "string";
            default: return "auto";
        }
    }

    getFlattenSpec = (allFields: Record<string, any>) => {
        return _.union(
            _.map(allFields, (field) => {
                return {
                    type: "path",
                    expr: field.expr,
                    name: field.name
                }
            }),
            rawIngestionSpecDefaults.flattenSpec
        )
    }
}

export const tableGenerator = new TableGenerator();