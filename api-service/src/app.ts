import express, { Application } from "express";
import { config } from "./v1/configs/Config";
import { ResponseHandler } from "./v1/helpers/ResponseHandler";
import { loadExtensions } from "./v1/managers/Extensions";
import { router } from "./v1/routes/Router";
import {router as v2Router} from "./v2/routes/Router"
import {router as metricsRouter} from "./v2/routes/metricRouter"
import bodyParser from "body-parser";
import { interceptAuditEvents } from "./v1/services/telemetry";
import { queryService } from "./v1/routes/Router";
import { routesConfig } from "./v1/configs/RoutesConfig";
import { QueryValidator } from "./v1/validators/QueryValidator";
import { errorHandler } from "./v2/middlewares/errors";
const app: Application = express();
const queryValidator = new QueryValidator();

const services = {
  queryService,
  validationService: queryValidator,
  nativeQueryId: routesConfig.query.native_query.api_id,
  sqlQueryId: routesConfig.query.sql_query.api_id,
}
 
app.use(bodyParser.json({ limit: config.body_parser_limit}));
app.use(express.text());
app.use(express.json());
app.use(errorHandler)
app.set("queryServices", services);

loadExtensions(app)
  .finally(() => {
    app.use(interceptAuditEvents())
    app.use("/v2/", v2Router);
    app.use("/", router);
    app.use("/", metricsRouter);
    app.use("*", ResponseHandler.routeNotFound);
    app.use(ResponseHandler.errorResponse);

    app.listen(config.api_port, () => {
      console.log(`listening on port ${config.api_port}`);
  });
});


export default app;
