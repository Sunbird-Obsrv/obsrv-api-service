export const TestInputsForDatasetRead = {
    DRAFT_SCHEMA: {
        "dataset_id": "sb-telemetry",
        "name": "sb-telemetry",
        "type": "event",
        "status": "Draft",
        "tags": [
            "tag1",
            "tag2"
        ],
        "version": 1,
        "api_version": "v2",
        "dataset_config": {
            "indexing_config": {
                "olap_store_enabled": false,
                "lakehouse_enabled": true,
                "cache_enabled": false
            },
            "keys_config": {
                "timestamp_key": "ets"
            },
            "file_upload_path": [
                "telemetry.json"
            ]
        }
    },
    LIVE_SCHEMA: {
        "dataset_id": "sb-telemetry",
        "name": "sb-telemetry",
        "type": "event",
        "status": "Live",
        "tags": [
            "tag1",
            "tag2"
        ],
        "data_version": 1,
        "api_version": "v2",
        "dataset_config": {
            "indexing_config": {
                "olap_store_enabled": false,
                "lakehouse_enabled": true,
                "cache_enabled": false
            },
            "keys_config": {
                "timestamp_key": "ets"
            },
            "file_upload_path": [
                "telemetry.json"
            ]
        }
    },
    TRANSFORMATIONS_SCHEMA:[{ "field_key": "eid", "transformation_function": { "type": "mask", "expr": "eid", "datatype": "string", "category": "pii" }, "mode": "Strict" }],
    TRANSFORMATIONS_SCHEMA_V1: [
        {
            "field_key": "eid",
            "transformation_function": {
                "type": "mask",
                "expr": "eid",
                "condition": null
            },
            "mode": "Strict",
            "metadata": {
                "_transformationType": "mask",
                "_transformedFieldDataType": "string",
                "_transformedFieldSchemaType": "string",
                "section": "transformation"
            }
        }
    ],
    DATASOURCE_SCHEMA: {
        "id": "sb-telemetry_sb-telemetry",
        "datasource": "sb-telemetry",
        "dataset_id": "sb-telemetry",
        "ingestion_spec": { "type": "kafka", "spec": { "dataSchema": { "dataSource": "dataset-conf_day", "dimensionsSpec": { "dimensions": [{ "type": "string", "name": "a" }, { "type": "string", "name": "obsrv.meta.source.connector" }, { "type": "string", "name": "obsrv.meta.source.id" }] }, "timestampSpec": { "column": "obsrv_meta.syncts", "format": "auto" }, "metricsSpec": [], "granularitySpec": { "type": "uniform", "segmentGranularity": "DAY", "queryGranularity": "none", "rollup": false } }, "tuningConfig": { "type": "kafka", "maxBytesInMemory": 134217728, "maxRowsPerSegment": 5000000, "logParseExceptions": true }, "ioConfig": { "type": "kafka", "consumerProperties": { "bootstrap.servers": "localhost:9092" }, "taskCount": 1, "replicas": 1, "taskDuration": "PT1H", "useEarliestOffset": true, "completionTimeout": "PT1H", "inputFormat": { "type": "json", "flattenSpec": { "useFieldDiscovery": true, "fields": [{ "type": "path", "expr": "$.['a']", "name": "a" }, { "type": "path", "expr": "$.obsrv_meta.['syncts']", "name": "obsrv_meta.syncts" }, { "type": "path", "expr": "$.obsrv_meta.source.['connector']", "name": "obsrv.meta.source.connector" }, { "type": "path", "expr": "$.obsrv_meta.source.['connectorInstance']", "name": "obsrv.meta.source.id" }, { "expr": "$.obsrv_meta.syncts", "name": "obsrv_meta.syncts", "type": "path" }] } }, "appendToExisting": false } } },
        "datasource_ref": "sb-telemetry_DAY",
        "retention_period": {
            "enabled": "false"
        },
        "archival_policy": {
            "enabled": "false"
        },
        "purge_policy": {
            "enabled": "false"
        },
        "backup_config": {
            "enabled": "false"
        },
        "status": "Live",
        "created_by": "SYSTEM",
        "updated_by": "SYSTEM",
        "published_date": "2023-07-03 00:00:00"
    }
}