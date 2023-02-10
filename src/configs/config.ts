import { queryRules } from "./queryRules";

export const config = {
  "api_port": process.env.api_port || 3000,

  "query_api": {
    "druid": {
      "host": process.env.druid_host || "http://localhost",
      "port": process.env.druid_port || 8888,
      "status_api": "/status",
      "health_api": "/status/health",
      "sql_query_path": "/druid/v2/sql/",
      "native_query_path": "/druid/v2"
    }
  },

  "postgres": {
    "pg_config": {
      host: 'localhost',
      port: 5432,
      database: 'postgres',
      user: 'manjunathdavanam',
      password: 'Manju@123',
    }
  },

  "datasetConfig": {
    "ingestion": {
      "dataset": "observ-demo",
      "indexCol": "ets",
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "hour",
        "rollup": true
      },

    },
    "querying": queryRules,
    "processing": {
      "checkpointingInterval": 6000,
      "dedupProperty": "mid",
      "dedupRetentionPeriod": 3600000,
      "consumerParallelism": 1,
      "downstreamParallelism": 1,
      "compression": "snappy",
      "dataSize": 1572864
    }
  }
}