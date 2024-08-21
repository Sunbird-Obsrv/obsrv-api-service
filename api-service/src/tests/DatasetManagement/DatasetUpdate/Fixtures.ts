export const requestStructure = {
    "id": "api.datasets.update",
    "ver": "v2",
    "ts": "2024-04-10T16:10:50+05:30",
    "params": {
        "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
    }
}

export const validVersionKey = "1713444815918"

export const TestInputsForDatasetUpdate = {

    MINIMAL_DATASET_UPDATE_REQUEST: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "telemetry",
            "sample_data":{"events":{}}
        }
    },

    DATASET_UPDATE_TAG_ADD: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "tags": [
                {
                    "value": "tag1",
                    "action": "upsert"
                }]
        }
    },

    DATASET_UPDATE_TAG_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "tags": [
                {
                    "value": "tag1",
                    "action": "remove"
                }]
        }
    },

    DATASET_UPDATE_DENORM_ADD: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "denorm_config": {
                "denorm_fields": [
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "userdata",
                            "dataset_id": "master"
                        },
                        "action": "upsert"
                    }
                ]
            }
        }
    },

    DATASET_UPDATE_DENORM_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "denorm_config": {
                "denorm_fields": [
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "userdata"
                        },
                        "action": "remove"
                    }
                ]
            }
        }
    },

    DATASET_UPDATE_TRANSFORMATIONS_ADD: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "transformations_config": [{ "value": { "field_key": "key1", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "upsert" }],
        }
    },

    DATASET_UPDATE_CONNECTORS_ADD: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "connectors_config":[{"value":{"id":"6c3fc8c2-357d-489b-b0c9-afdde6e5c6c0","connector_id":"kafka","connector_config":{"type":"kafka","topic":"telemetry.ingest","kafkaBrokers":"kafka-headless.kafka.svc:9092"},"version":"v1"}, "action": "upsert"}],
        }
    },

    DATASET_UPDATE_CONNECTORS_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "connectors_config":[{"value":{"id":"6c3fc8c2-357d-489b-b0c9-afdde6e5c6c0","connector_id":"kafka","connector_config":{"type":"kafka","topic":"telemetry.ingest","kafkaBrokers":"kafka-headless.kafka.svc:9092"},"version":"v1"}, "action": "upsert"}],
        }
    },

    DATASET_UPDATE_DEDUP_DUPLICATES_TRUE: {
        ...requestStructure,
        request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "dedup_config": {
                "drop_duplicates": true,
                "dedup_key": "mid"
            }
        }
    },

    DATASET_UPDATE_EXTRACTION_DROP_DUPLICATES: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "extraction_config": {
                "is_batch_event": true,
                "extraction_key": "events",
                "dedup_config": {
                    "drop_duplicates": true,
                    "dedup_key": "id"
                }
            }
        }
    },

    DATASET_UPDATE_VALIDATION_VALIDATE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "validation_config": {
                "validate": true,
                "mode": "Strict"
            }
        }
    },

    DATASET_UPDATE_DATA_SCHEMA_VALID: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "data_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "eid": {
                        "type": "string"
                    },
                    "ver": {
                        "type": "string"
                    },
                    "required": [
                        "eid"
                    ]
                },
                "additionalProperties": true
            }
        }
    },

    DATASET_WITH_INVALID_TIMESTAMP: {
        ...requestStructure,
        request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "sb-telemetry",
            "data_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "eid": {
                        "type": "string"
                    },
                    "ver": {
                        "type": "string"
                    },
                    "required": [
                        "eid"
                    ]
                },
                "additionalProperties": true
            },
            "dataset_config": {
                "data_key": "eid",
                "timestamp_key": "ets",
                "file_upload_path": ["/config/file.json"]
            }
        }
    },

    DATASET_UPDATE_DATASET_CONFIG_VALID: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "dataset_config": {
                "indexing_config": {
                    "olap_store_enabled": false,
                    "lakehouse_enabled": true,
                    "cache_enabled": false
                },
                "keys_config": {
                    "timestamp_key": "ets",
                    "data_key": "ets"
                },
                "file_upload_path": [
                    "telemetry.json"
                ]
            }
        }
    },

    DATASET_UPDATE_TRANSFORMATIONS_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "transformations_config": [{ "value": { "field_key": "key1", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "upsert" }],
        }
    },

    DATASET_UPDATE_TRANSFORMATIONS_UPDATE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "transformation_config": [
                {
                    "values": {
                        "field_key": "key1",
                        "transformation_function": {},
                        "mode": "Strict",
                        "metadata": {}
                    },
                    "action": "update"
                }]
        }
    },

    DATASET_UPDATE_REQUEST: {
        ...requestStructure,
        request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "sb-telemetry",
            "validation_config": {
                "validate": true,
                "mode": "Strict"
            },
            "extraction_config": {
                "is_batch_event": true,
                "extraction_key": "events",
                "dedup_config": {
                    "drop_duplicates": true,
                    "dedup_key": "id"
                }
            },
            "dedup_config": {
                "drop_duplicates": true,
                "dedup_key": "mid"
            },
            "data_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "eid": {
                        "type": "string"
                    },
                    "ets": {
                        "type": "string"
                    },
                    "ver": {
                        "type": "string"
                    },
                    "required": [
                        "eid"
                    ]
                },
                "additionalProperties": true
            },
            "denorm_config": {
                "denorm_fields": [
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "userdata",
                            "dataset_id": "master"
                        },
                        "action": "upsert"
                    },
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "mid",
                            "dataset_id": "master"
                        },
                        "action": "remove"
                    }
                ]
            },
            "transformations_config": [{ "value": { "field_key": "key1", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "upsert" }, { "value": { "field_key": "key2", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "remove" }],
            "dataset_config": {
                "indexing_config": {
                    "olap_store_enabled": false,
                    "lakehouse_enabled": true,
                    "cache_enabled": false
                },
                "keys_config": {
                    "timestamp_key": "ets",
                    "data_key": "ets"
                },
                "file_upload_path": [
                    "telemetry.json"
                ]
            },
            "tags": [
                {
                    "value": "tag1",
                    "action": "remove"
                },
                {
                    "value": "tag3",
                    "action": "upsert"
                }
            ]
        }
    },

    DATASET_UPDATE_DUPLICATE_DENORM_KEY: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "denorm_config": {
                "denorm_fields": [
                    {
                        "values": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "userdata"
                        },
                        "action": "upsert"
                    },
                    {
                        "values": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "userdata"
                        },
                        "action": "upsert"
                    }
                ]
            }
        }
    },

    DATASET_UPDATE_WITH_SAME_DENORM_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "sb-telemetry",
            "denorm_config": {
                "denorm_fields": [
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "mid",
                            "dataset_id": "master"
                        },
                        "action": "remove"
                    },
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "mid",
                            "dataset_id": "master"
                        },
                        "action": "remove"
                    }
                ]
            }
        }
    },

    DATASET_UPDATE_WITH_EXISTING_DENORM: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "sb-telemetry",
            "denorm_config": {
                "denorm_fields": [
                    {
                        "value": {
                            "denorm_key": "actor.id",
                            "denorm_out_field": "mid",
                            "dataset_id": "master"
                        },
                        "action": "upsert"
                    }
                ]
            }
        }
    },

    DATASET_UPDATE_WITH_SAME_TRANSFORMATION_ADD_REMOVE: {
        ...requestStructure, request: {
            "dataset_id": "telemetry",
            "version_key": validVersionKey,
            "name": "sb-telemetry",
            "transformations_config": [{ "value": { "field_key": "key1", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "upsert" }, { "value": { "field_key": "key1", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }, "action": "upsert" }]
        }
    }
}

export const msgid = "4a7f14c3-d61e-4d4f-be78-181834eeff6d"