export const TestInputsForConnectorsList = {
    INVALID_REQUEST: {
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-04-10T16:10:50+05:30",
        "params": {
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
        },
        "request": {
            "filters": { category: [] }
        }
    },
    REQUEST_WITHOUT_FILTERS: {
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-04-10T16:10:50+05:30",
        "params": {
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
        },
        "request": {}
    },
    REQUEST_WITH_STATUS_FILTERS: {
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-04-10T16:10:50+05:30",
        "params": {
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
        },
        "request": {
            "filters": { status: ["Live"] }
        }
    },
    REQUEST_WITH_CATEGORY_FILTERS: {
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-04-10T16:10:50+05:30",
        "params": {
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
        },
        "request": {
            "filters": {
                "category": [
                    "Database"
                ]
            }
        }
    },
    REQUEST_WITH_BOTH_FILTERS: {
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-04-10T16:10:50+05:30",
        "params": {
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d"
        },
        "request": {
            "filters": { category: ["Database"], status: ["Live"] }
        }
    },

    VALID_CONNECTORS_LIST:{
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-07-30T15:17:51+05:30",
        "params": {
            "status": "SUCCESS",
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d",
            "resmsgid": "fffa3ee0-da12-4bea-9b72-365571a62a4e"
        },
        "responseCode": "OK",
        "result": {
            "data": [
                {
                    "id": "postgres-connector-1.0.0",
                    "connector_id": "postgres-connector",
                    "name": "PostgreSQL",
                    "type": "source",
                    "category": "Database",
                    "version": "1.0.0",
                    "description": "The PostgreSQL Connector is used to move data from any Postgres Table to the Obsrv platform",
                    "technology": "scala",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:38:28.732Z",
                    "updated_date": "2024-06-25T04:38:28.732Z"
                },
                {
                    "id": "mysql-connector-1.0.0",
                    "connector_id": "mysql-connector",
                    "name": "MySQL",
                    "type": "source",
                    "category": "Database",
                    "version": "1.0.0",
                    "description": "The MySQL Connector is used to move data from any MySQL Table to the Obsrv platform",
                    "technology": "scala",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/en/6/62/MySQL.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:38:28.834Z",
                    "updated_date": "2024-06-25T04:38:28.834Z"
                },
                {
                    "id": "oracle-connector-1.0.0",
                    "connector_id": "oracle-connector",
                    "name": "Oracle",
                    "type": "source",
                    "category": "Database",
                    "version": "1.0.0",
                    "description": "The Oracle Connector is used to move data from any Oracle Table to the Obsrv platform",
                    "technology": "scala",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/5/50/Oracle_logo.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:38:28.840Z",
                    "updated_date": "2024-06-25T04:38:28.840Z"
                },
                {
                    "id": "mssql-connector-1.0.0",
                    "connector_id": "mssql-connector",
                    "name": "MS SQL",
                    "type": "source",
                    "category": "Database",
                    "version": "1.0.0",
                    "description": "The MS SQL Connector is used to move data from any MS SQL Table to the Obsrv platform",
                    "technology": "scala",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/2/29/Microsoft_SQL_Server_Logo.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:38:28.847Z",
                    "updated_date": "2024-06-25T04:38:28.847Z"
                },
                {
                    "id": "aws-s3-connector-0.1.0",
                    "connector_id": "aws-s3-connector",
                    "name": "AWS S3",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The AWS S3 Connector is used to move data from any S3 Bucket to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/b/bc/Amazon-S3-Logo.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.237Z",
                    "updated_date": "2024-06-25T04:39:21.237Z"
                },
                {
                    "id": "azure-blob-connector-0.1.0",
                    "connector_id": "azure-blob-connector",
                    "name": "Azure Blob Store",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The Azure Blob Store Connector is used to move data from any Azure Blob Container to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/f/fa/Microsoft_Azure.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.302Z",
                    "updated_date": "2024-06-25T04:39:21.302Z"
                },
                {
                    "id": "gcs-connector-0.1.0",
                    "connector_id": "gcs-connector",
                    "name": "Google Cloud Storage",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The GCS Connector is used to move data from any Google Bucket to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/51/Google_Cloud_logo.svg/512px-Google_Cloud_logo.svg.png",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.364Z",
                    "updated_date": "2024-06-25T04:39:21.364Z"
                }
            ],
            "count": 7
        }
    },
    VALID_CONNECTORS_LIST_CATEGORY:{
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-07-30T15:07:42+05:30",
        "params": {
            "status": "SUCCESS",
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d",
            "resmsgid": "632d3342-fd8a-47f7-afbb-96402a00b92f"
        },
        "responseCode": "OK",
        "result": {
            "data": [
                {
                    "id": "aws-s3-connector-0.1.0",
                    "connector_id": "aws-s3-connector",
                    "name": "AWS S3",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The AWS S3 Connector is used to move data from any S3 Bucket to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/b/bc/Amazon-S3-Logo.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.237Z",
                    "updated_date": "2024-06-25T04:39:21.237Z"
                },
                {
                    "id": "azure-blob-connector-0.1.0",
                    "connector_id": "azure-blob-connector",
                    "name": "Azure Blob Store",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The Azure Blob Store Connector is used to move data from any Azure Blob Container to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/f/fa/Microsoft_Azure.svg",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.302Z",
                    "updated_date": "2024-06-25T04:39:21.302Z"
                },
                {
                    "id": "gcs-connector-0.1.0",
                    "connector_id": "gcs-connector",
                    "name": "Google Cloud Storage",
                    "type": "source",
                    "category": "File",
                    "version": "0.1.0",
                    "description": "The GCS Connector is used to move data from any Google Bucket to the Obsrv platform",
                    "technology": "python",
                    "runtime": "spark",
                    "licence": "MIT",
                    "owner": "Sunbird",
                    "iconurl": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/51/Google_Cloud_logo.svg/512px-Google_Cloud_logo.svg.png",
                    "status": "Live",
                    "created_by": "SYSTEM",
                    "updated_by": "SYSTEM",
                    "created_date": "2024-06-25T04:39:21.364Z",
                    "updated_date": "2024-06-25T04:39:21.364Z"
                }
            ],
            "count": 3
        }
    },
    VALID_CONNECTORS_LIST_STATUS:{
        "id": "api.connectors.list",
        "ver": "v2",
        "ts": "2024-07-30T15:25:51+05:30",
        "params": {
            "status": "SUCCESS",
            "msgid": "4a7f14c3-d61e-4d4f-be78-181834eeff6d",
            "resmsgid": "f506e725-eed4-41df-86dc-2477d5c4d19a"
        },
        "responseCode": "OK",
        "result": {
            "data": [],
            "count": 0
        }
    }
}

