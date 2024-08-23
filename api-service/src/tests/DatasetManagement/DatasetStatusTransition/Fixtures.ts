export const TestInputsForDatasetStatusTransition = {
  VALID_SCHEMA_FOR_DELETE: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "telemetry.1",
      "status": "Delete"
    }
  },
  VALID_SCHEMA_FOR_LIVE: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "telemetry",
      "status": "Live"
    }
  },
  VALID_SCHEMA_FOR_LIVE_MASTER: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "master-telemetry",
      "status": "Live"
    }
  },
  VALID_SCHEMA_FOR_RETIRE: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "telemetry",
      "status": "Retire"
    }
  },
  INVALID_SCHEMA: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "telemetry.1",
      "status": ""
    }
  },
  VALID_REQUEST_FOR_READY_FOR_PUBLISH: {
    "id": "api.datasets.status-transition",
    "ver": "v2",
    "ts": "2024-04-19T12:58:47+05:30",
    "params": {
      "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6"
    },
    "request": {
      "dataset_id": "telemetry",
      "status": "ReadyToPublish"
    }
  },
  VALID_SCHEMA_FOR_READY_TO_PUBLISH: {
    "id": "dataset-all-fields7",
    "dataset_id": "dataset-all-fields7",
    "version": 1,
    "type": "event",
    "name": "sb-telemetry",
    "validation_config": { "validate": false, "mode": "Strict" },
    "extraction_config": { "is_batch_event": true, "extraction_key": "events", "dedup_config": { "drop_duplicates": true, "dedup_key": "id", "dedup_period": 604800 } },
    "dedup_config": { "drop_duplicates": true, "dedup_key": "mid", "dedup_period": 604800 },
    "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "mid": { "type": "string", "arrival_format": "text", "data_type": "string" }, "ets": { "type": "integer", "arrival_format": "number", "data_type": "epoch" }, "eid": { "type": "string", "arrival_format": "text", "data_type": "string" } }, "additionalProperties": true },
    "denorm_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "denorm_fields": [{ "denorm_key": "eid", "denorm_out_field": "userdata", "dataset_id": "master-dataset", "redis_db": 85 }] },
    "router_config": { "topic": "dataset-all-fields7" },
    "dataset_config": { "indexing_config": { "olap_store_enabled": true, "lakehouse_enabled": true, "cache_enabled": false }, "keys_config": { "data_key": "eid", "partition_key": "eid", "timestamp_key": "obsrv_meta.syncts" }, "cache_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "redis_db": 0 }, "file_upload_path": [] },
    "tags": ["tag1"],
    "status": "Draft",
    "created_by": "SYSTEM",
    "updated_by": "SYSTEM",
    "created_date": "2024-07-24 19:12:13.021",
    "updated_date": "2024-07-25 06:12:38.412",
    "version_key": "1721887933020",
    "api_version": "v2",
    "transformations_config": [{ "field_key": "email", "transformation_function": { "type": "mask", "expr": "mid", "datatype": "string", "category": "pii" }, "mode": "Strict" }],
    "connectors_config": [{ "id": "91898e828u82882u8", "connector_id": "kafka", "connector_config": "AR/hz8iBxRyc9s0ohXa3+id+7GoWtjVjNWvurFFgV1Ocw2kgc+XVbnfXX26zkP3+rQ49gio0JzwsFzOK61TtXLx968IKol5eGfaEHF68O5faoxxjKBsyvhPaRQ91DKKi", "version": "v1" }],
    "sample_data": {},
    "entry_topic": "local.ingest"
  },
  VALID_MASTER_SCHEMA_FOR_READY_TO_PUBLISH: {
    "id": "dataset-all-fields7",
    "dataset_id": "dataset-all-fields7",
    "version": 1,
    "type": "master",
    "name": "sb-telemetry",
    "validation_config": { "validate": false, "mode": "Strict" },
    "extraction_config": { "is_batch_event": true, "extraction_key": "events", "dedup_config": { "drop_duplicates": true, "dedup_key": "id", "dedup_period": 604800 } },
    "dedup_config": { "drop_duplicates": true, "dedup_key": "mid", "dedup_period": 604800 },
    "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "mid": { "type": "string", "arrival_format": "text", "data_type": "string" }, "ets": { "type": "integer", "arrival_format": "number", "data_type": "epoch" }, "eid": { "type": "string", "arrival_format": "text", "data_type": "string" } }, "additionalProperties": true },
    "denorm_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "denorm_fields": [{ "denorm_key": "eid", "denorm_out_field": "userdata", "dataset_id": "master-dataset", "redis_db": 85 }] },
    "router_config": { "topic": "dataset-all-fields7" },
    "dataset_config": { "indexing_config": { "olap_store_enabled": true, "lakehouse_enabled": true, "cache_enabled": false }, "keys_config": { "data_key": "eid", "partition_key": "eid", "timestamp_key": "obsrv_meta.syncts" }, "cache_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "redis_db": 0 }, "file_upload_path": [] },
    "tags": ["tag1"],
    "status": "Draft",
    "created_by": "SYSTEM",
    "updated_by": "SYSTEM",
    "created_date": "2024-07-24 19:12:13.021",
    "updated_date": "2024-07-25 06:12:38.412",
    "version_key": "1721887933020",
    "transformations_config": [],
    "connectors_config": [],
    "api_version": "v2",
    "sample_data": {},
    "entry_topic": "local.ingest"
  },
  INVALID_SCHEMA_FOR_READY_TO_PUBLISH: {
    "id": "dataset-all-fields7",
    "dataset_id": "dataset-all-fields7",
    "version": 1,
    "type": "event",
    "name": "sb-telemetry",
    "validation_config": { "validate": false, "mode": "Strict" },
    "extraction_config": { "is_batch_event": true, "extraction_key": "events", "dedup_config": { "drop_duplicates": true, "dedup_key": "id", "dedup_period": 604800 } },
    "dedup_config": { "drop_duplicates": true, "dedup_key": "mid", "dedup_period": 604800 },
    "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "mid": { "type": "string", "arrival_format": "text", "data_type": "string" }, "ets": { "type": "integer", "arrival_format": "number", "data_type": "epoch" }, "eid": { "type": "string", "arrival_format": "text", "data_type": "string" } }, "additionalProperties": true },
    "denorm_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "denorm_fields": [{ "denorm_key": "eid", "denorm_out_field": "userdata", "dataset_id": "master-dataset", "redis_db": 85 }] },
    "router_config": { "topic": "dataset-all-fields7" },
    "tags": ["tag1"],
    "status":"Draft",
    "version_key": "1721887933020",
    "api_version": "v2"
  },
  SCHEMA_TO_RETIRE: {
    "id": "dataset-all-fields7",
    "dataset_id": "dataset-all-fields7",
    "version": 1,
    "type": "event",
    "name": "sb-telemetry",
    "validation_config": { "validate": false, "mode": "Strict" },
    "extraction_config": { "is_batch_event": true, "extraction_key": "events", "dedup_config": { "drop_duplicates": true, "dedup_key": "id", "dedup_period": 604800 } },
    "dedup_config": { "drop_duplicates": true, "dedup_key": "mid", "dedup_period": 604800 },
    "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "mid": { "type": "string", "arrival_format": "text", "data_type": "string" }, "ets": { "type": "integer", "arrival_format": "number", "data_type": "epoch" }, "eid": { "type": "string", "arrival_format": "text", "data_type": "string" } }, "additionalProperties": true },
    "denorm_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "denorm_fields": [{ "denorm_key": "eid", "denorm_out_field": "userdata", "dataset_id": "master-dataset", "redis_db": 85 }] },
    "router_config": { "topic": "dataset-all-fields7" },
    "tags": ["tag1"],
    "status":"Live",
    "version_key": "1721887933020",
    "api_version": "v2"
  },
  DRAFT_DATASET_SCHEMA_FOR_PUBLISH: { "dataset_id": "telemetry", "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "ets": { "type": "string" }, "ver": { "type": "string" } }, "additionalProperties": true }, "status": "ReadyToPublish", "id": "telemetry", "type": "events", "api_version": "v2", "denorm_config": { "denorm_fields": [{ "denorm_out_field": "pid", "denorm_key": "eid", "dataset_id": "master-dataset" }] }, "dataset_config": { "indexing_config": { "olap_store_enabled": true, "lakehouse_enabled": false, "cache_enabled": false }, "keys_config": { "timestamp_key": "ets", "partition_key": "", "data_key": "eid" }, "file_upload_path": ["telemetry.json"] }, "router_config": { "topic": "telemetry" } },
  DRAFT_DATASET_SCHEMA_FOR_PUBLISH_HUDI: { "dataset_id": "telemetry", "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "ets": { "type": "string" }, "ver": { "type": "string" } }, "additionalProperties": true }, "status": "ReadyToPublish", "id": "telemetry", "type": "events", "api_version": "v2", "denorm_config": { "denorm_fields": [{ "denorm_out_field": "pid", "denorm_key": "eid", "dataset_id": "master-dataset" }] }, "dataset_config": { "indexing_config": { "olap_store_enabled": false, "lakehouse_enabled": true, "cache_enabled": false }, "keys_config": { "timestamp_key": "ets", "partition_key": "ets", "data_key": "eid" }, "file_upload_path": ["telemetry.json"] }, "router_config": { "topic": "telemetry" } },
  DRAFT_MASTER_DATASET_SCHEMA_FOR_PUBLISH: { "dataset_id": "master-telemetry", "data_schema": { "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "properties": { "ets": { "type": "string" }, "ver": { "type": "string" } }, "additionalProperties": true }, "status": "ReadyToPublish", "id": "master-telemetry", "type": "master", "api_version": "v2", "denorm_config": { "denorm_fields": [] }, "dataset_config": { "indexing_config": { "olap_store_enabled": false, "lakehouse_enabled": false, "cache_enabled": true }, "keys_config": { "timestamp_key": "ets", "partition_key": "", "data_key": "eid" }, "cache_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "redis_db": 0 }, "file_upload_path": ["telemetry.json"] }, "router_config": { "topic": "telemetry" } },
  DRAFT_MASTER_DATASET_INVALID: { "dataset_id": "master-telemetry", "status": "ReadyToPublish", "id": "master-telemetry", "type": "master", "api_version": "v2", "denorm_config": { "denorm_fields": [] }, "dataset_config": { "indexing_config": { "olap_store_enabled": false, "lakehouse_enabled": false, "cache_enabled": true }, "keys_config": { "timestamp_key": "ets", "partition_key": "", "data_key": "eid" }, "cache_config": { "redis_db_host": "localhost", "redis_db_port": 5679, "redis_db": 0 }, "file_upload_path": ["telemetry.json"] }, "router_config": { "topic": "telemetry" } }
}