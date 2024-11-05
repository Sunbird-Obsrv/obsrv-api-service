import express, { Application } from "express";
import {router as v2Router} from "./routes/Router"
import { metricRouter } from "./routes/MetricRouter"
import { druidProxyRouter } from "./routes/DruidProxyRouter"

import bodyParser from "body-parser";
import { errorHandler, obsrvErrorHandler } from "./middlewares/errors";
import { ResponseHandler } from "./helpers/ResponseHandler";
import { config } from "./configs/Config";
import { alertsRouter } from "./routes/AlertsRouter";
import { interceptAuditEvents } from "./services/telemetry";
import { OTelService } from "./otel/OTelService";
import { LogRecord } from "@opentelemetry/sdk-logs";



const app: Application = express();
OTelService.init()
OTelService.log()

const auditLog = {
  "eid": "AUDIT",
  "ets": 1729158293107,
  "ver": "1.0.0",
  "mid": "759b9471-b3bd-4818-89f5-cbdf5cdfc421",
  "actor": {
      "id": "SYSTEM",
      "type": "User"
  },
  "context": {
      "env": "local",
      "sid": "37229d4c-38a1-4aac-94cf-5fe34230fb1a",
      "pdata": {
          "id": "local.api.service",
          "ver": "1.0"
      }
  },
  "object": {},
  "edata": {
      "action": "dataset:create",
      "props": [
          {
              "property": "id",
              "ov": null,
              "nv": "api.data.in"
          },
          {
              "property": "ver",
              "ov": null,
              "nv": "v2"
          },
          {
              "property": "ts",
              "ov": null,
              "nv": "1711966306164"
          },
          {
              "property": "params",
              "ov": null,
              "nv": {
                  "msgid": "e180ecac-8f41-4f21-9a21-0b3a1a368917"
              }
          },
          {
              "property": "data",
              "ov": null,
              "nv": {
                  "eid": "INTERACT",
                  "date": "2022-01-01",
                  "ver": "3.0",
                  "syncts": 1668591949682,
                  "ets": 1668591949682
              }
          }
      ],
      "transition": {
          "timeUnit": "ms",
          "duration": 437,
          "toState": "completed",
          "fromState": "inprogress"
      }
  }
};

// Emit the audit log
OTelService.emitAuditLog(auditLog);
//const loggerInstance = OTelService.getLoggerProvider();





app.use(bodyParser.json({ limit: config.body_parser_limit}));
app.use(express.text());
app.use(express.json());
app.use(errorHandler)

app.use(interceptAuditEvents());
app.use("/v2/", v2Router);
app.use("/", druidProxyRouter);
app.use("/alerts/v1", alertsRouter);
app.use("/", metricRouter);
app.use(/(.*)/, ResponseHandler.routeNotFound);
app.use(obsrvErrorHandler);

app.listen(config.api_port, () => {
  console.log(`listening on port ${config.api_port}`);
});

export default app;