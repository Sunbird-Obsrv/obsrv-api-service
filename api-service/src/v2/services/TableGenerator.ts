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
    flattenSchema = (dataSchema: Record<string, any>, type: string) : Record<string, any>[] => {

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
                        if(type === "druid" && _.get(value, 'items.type') == 'object' && _.get(value, 'items.properties')) {
                            _.mapKeys(_.get(value, 'items.properties'), function(value, childKey) {
                                const objChildKey = _.join([newKey, childKey], '.')
                                properties.push(_.merge(_.pick(value, ['type', 'arrival_format', 'is_deleted']), {expr: _.join([newExpr,"[*].['",childKey,"']"], ''), name: objChildKey, data_type: 'array'}))
                            })
                        } else {
                            properties.push(_.merge(_.pick(value, ['arrival_format', 'data_type', 'is_deleted']), {expr: newExpr+'[*]', name: newKey, type: _.get(value, 'items.type')}))
                        }
                        break;
                    default:
                        properties.push(_.merge(_.pick(value, ['type', 'arrival_format', 'data_type', 'is_deleted']), {expr: newExpr, name: newKey}))
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
    getAllFields = async (dataset: Record<string, any>, type: string) : Promise<Record<string, any>[]> => {

        const { data_schema, denorm_config, transformations_config} = dataset
        const instance = this;
        let dataFields = instance.flattenSchema(data_schema, type);
        if(denorm_config.denorm_fields) {
            _.map(denorm_config.denorm_fields, async (denormField) => {
                const denormDataset: any = await datasetService.getDataset(denormField.dataset_id, ["data_schema"], true);
                const properties = instance.flattenSchema(denormDataset.data_schema, type);
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
                    data_type: tf.transformation_function.datatype,
                    arrival_format: tf.transformation_function.datatype,
                    type: tf.transformation_function.datatype
                })
            })
        }
        dataFields.push(rawIngestionSpecDefaults.synctsField)
        _.remove(dataFields, {is_deleted: true}) // Delete all the excluded fields
        return dataFields;
    }
}

class TableGenerator extends BaseTableGenerator {

    getDruidIngestionSpec = (dataset: Record<string, any>, allFields: Record<string, any>[], datasourceRef: string) => {
        
        const { dataset_config, router_config } = dataset
        return {
            "type": "kafka",
            "spec": {
                "dataSchema": {
                    "dataSource": datasourceRef,
                    "dimensionsSpec": { "dimensions": this.getDruidDimensions(allFields, this.getTimestampKey(dataset), dataset_config.keys_config.partition_key) },
                    "timestampSpec": { "column": this.getTimestampKey(dataset), "format": "auto" },
                    "metricsSpec": [],
                    "granularitySpec": rawIngestionSpecDefaults.granularitySpec
                },
                "tuningConfig": rawIngestionSpecDefaults.tuningConfig,
                "ioConfig": _.merge(rawIngestionSpecDefaults.ioConfig, {
                    "topic": router_config.topic,
                    "inputFormat": {
                        "flattenSpec": {
                            "fields": this.getDruidFlattenSpec(allFields)
                        }
                    }
                })
            }
        }
    }
    
    getDruidDimensions = (allFields: Record<string, any>[], timestampKey: string, partitionKey: string | undefined) => {

        const dataFields = _.cloneDeep(allFields);
        if(partitionKey) { // Move the partition column to the top of the dimensions
            const partitionCol = _.remove(dataFields, {name: partitionKey})
            if(partitionCol && _.size(partitionCol) > 0) {
                dataFields.unshift(partitionCol[0])
            }
        }
        _.remove(dataFields, {name: timestampKey})
        const instance = this;
        return _.union(
            _.map(dataFields, (field) => {
                return {
                    "type": instance.getDruidDimensionType(field.data_type),
                    "name": field.name
                }
            }),
            rawIngestionSpecDefaults.dimensions
        )
    }

    getDruidDimensionType = (data_type: string):string => {
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

    getDruidFlattenSpec = (allFields: Record<string, any>) => {
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

    getHudiIngestionSpecForCreate = (dataset: Record<string, any>, allFields: Record<string, any>[], datasourceRef: string) => {

        const primaryKey = this.getPrimaryKey(dataset);
        const partitionKey = this.getHudiPartitionKey(dataset);
        const timestampKey = this.getTimestampKey(dataset);
        return {
            dataset: dataset.dataset_id,
            schema: {
                table: datasourceRef,
                partitionColumn: partitionKey,
                timestampColumn: timestampKey,
                primaryKey: primaryKey,
                columnSpec: this.getHudiColumnSpec(allFields, primaryKey, partitionKey, timestampKey)
            },
            inputFormat: {
                type: "json",
                flattenSpec: {
                    fields: this.getHudiFields(allFields)
                }
            }
        }
    }

    getHudiColumnSpec = (allFields: Record<string, any>[], primaryKey: string, partitionKey: string, timestampKey: string) : Record<string, any>[] => {

        const instance = this;
        const dataFields = _.cloneDeep(allFields);
        _.remove(dataFields, {name: primaryKey})
        _.remove(dataFields, {name: partitionKey})
        _.remove(dataFields, {name: timestampKey})
        let index = 1;
        const transformFields = _.map(dataFields, (field) => { 
            return {
                "type": instance.getHudiColumnType(field),
                "name": field.name,
                "index": index++
            }
        })
        _.each(rawIngestionSpecDefaults.dimensions, (field) => {
            transformFields.push({
                "type": field.type,
                "name": field.name,
                "index": index++
            })
        })
        return transformFields;
    }

    getHudiColumnType = (field: Record<string, any>) : string => {
        if(field.data_type === 'array' && field.arrival_format !== 'array') {
            return "array";
        }
        if(field.data_type === 'array' && field.arrival_format === 'array') {
            switch(field.type) {
                case "string":
                    return "array<string>"
                case "number":
                    return "array<double>"
                case "integer":
                    return "array<int>"
                case "boolean":
                    return "array<boolean>"
                default:
                    return "array<object>"
            }
        }
        switch(field.arrival_format) {
            case "text":
                return "string"
            case "number":
                switch(field.data_type) {
                    case "integer":
                        return "int"
                    case "epoch":
                        return "epoch"
                    case "bigdecimal":
                        return "bigdecimal"
                    case "float":
                        return "float"
                    case "long":
                        return "long"
                    default:
                        return "double"   
                }
            case "integer":
                return "int"
            case "boolean":
                return "boolean"
            default:
                return "string"
        }
    }

    getHudiFields = (allFields: Record<string, any>[]) : Record<string, any>[] => {

        return _.union(
            _.map(allFields, (field) => {
                return {
                    type: "path",
                    expr: _.replace(field.expr, /[\[\]'\*]/g, ""),
                    name: field.name
                }
            }),
            rawIngestionSpecDefaults.flattenSpec
        )
    }

    getPrimaryKey = (dataset: Record<string, any>) : string => {
        return dataset.dataset_config.keys_config.data_key;
    }

    getHudiPartitionKey = (dataset: Record<string, any>) : string => {
        return dataset.dataset_config.keys_config.partition_key || dataset.dataset_config.keys_config.timestamp_key;
    }

    getTimestampKey = (dataset: Record<string, any>) : string => {
        return dataset.dataset_config.keys_config.timestamp_key;
    }
}

export const tableGenerator = new TableGenerator();

const schema = '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"userid":{"type":"string","arrival_format":"text","data_type":"string"},"block":{"type":"string","arrival_format":"text","data_type":"string"},"cluster":{"type":"string","arrival_format":"text","data_type":"string"},"schooludisecode":{"type":"string","arrival_format":"text","data_type":"string"},"schoolname":{"type":"string","arrival_format":"text","data_type":"string"},"usertype":{"type":"string","arrival_format":"text","data_type":"string"},"usersubtype":{"type":"string","arrival_format":"text","data_type":"string"},"board":{"type":"string","arrival_format":"text","data_type":"string"},"rootorgid":{"type":"string","arrival_format":"text","data_type":"string"},"orgname":{"type":"string","arrival_format":"text","data_type":"string"},"subject":{"type":"array","items":{"type":"string"},"arrival_format":"array","data_type":"array"},"language":{"type":"array","items":{"type":"string"},"arrival_format":"array","data_type":"array"},"grade":{"type":"array","items":{"type":"string"},"arrival_format":"array","data_type":"array"},"framework":{"type":"string","arrival_format":"text","data_type":"string"},"medium":{"type":"array","items":{"type":"string"},"arrival_format":"array","data_type":"array"},"district":{"type":"string","arrival_format":"text","data_type":"string"},"profileusertypes":{"type":"array","items":{"type":"object","properties":{"type":{"type":"string","arrival_format":"text","data_type":"string"},"subType":{"type":"string","arrival_format":"text","data_type":"string"}},"additionalProperties":false},"arrival_format":"array","data_type":"array"}},"additionalProperties":false}';
const dataSchema = JSON.parse(schema);
const allFields = tableGenerator.flattenSchema(dataSchema, "druid")
const dataset = {
    dataset_id: "ny_trip_data",
    router_config: {
        topic: "ny_trip_data"
    },
    dataset_config: {
        keys_config: {
            data_key: "userid",
            timestamp_key: "grade",
            partition_key: "block",
        }
    }
}
console.log(JSON.stringify(tableGenerator.getDruidIngestionSpec(dataset, allFields, "ny_trip_data_events")))