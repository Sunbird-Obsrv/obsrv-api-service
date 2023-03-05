/** TODO: Delete this file and read query rules from database. Create a schema accordingly */
export const queryRules = {
  "common": {
    "maxResultThreshold": 100,
    "maxResultRowLimit": 100
  },
  "rules": [
    {
      "dataset": "telemetry-events",
      "queryRules": {
        "groupBy": {
          "maxDateRange": 30
        },
        "scan": {
          "maxDateRange": 30
        },
        "search": {
          "maxDateRange": 30
        },
        "timeBoundary": {
          "maxDateRange": 30
        },
        "timeseries": {
          "maxDateRange": 30
        },
        "topN": {
          "maxDateRange": 30
        }
      }
    },
    {
      "dataset": "summary-events",
      "queryRules": {
        "groupBy": {
          "maxDateRange": 30
        },
        "scan": {
          "maxDateRange": 30
        },
        "search": {
          "maxDateRange": 30
        },
        "timeBoundary": {
          "maxDateRange": 30
        },
        "timeseries": {
          "maxDateRange": 30
        },
        "topN": {
          "maxDateRange": 30
        }
      }
    },
    {
      "dataset": "",
      "queryRules": {
        "groupBy": {
          "maxDateRange": 30
        },
        "scan": {
          "maxDateRange": 30
        },
        "search": {
          "maxDateRange": 30
        },
        "timeBoundary": {
          "maxDateRange": 30
        },
        "timeseries": {
          "maxDateRange": 30
        },
        "topN": {
          "maxDateRange": 30
        }
      }
    }
  ]
}
