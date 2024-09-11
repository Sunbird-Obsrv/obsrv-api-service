import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { config } from "../configs/Config";
import _ from "lodash";

enum roles {
  OperationsAdmin = "operations_admin",
  Admin = "admin",
  DatasetManager = "dataset_manager",
  Viewer = "viewer",
  DatasetCreator = "dataset_creator",
  Ingestor = "ingestor",
}

enum permissions {
  AlertCreate = "api.alert.create",
  AlertPublish = "api.alert.publish",
  AlertUpdate = "api.alert.update",
  AlertList = "api.alert.list",
  AlertDelete = "api.alert.delete",
  AlertDetails = "api.alert.getAlertDetails",
  MetricCreate = "api.metric.add",
  MetricList = "api.metric.list",
  MetricUpdate = "api.metric.update",
  MetricRemove = "api.metric.remove",
  SilenceCreate = "api.alert.silence.create",
  SilenceList = "api.alert.silence.list",
  SilenceRead = "api.alert.silence.get",
  SilenceEdit = "api.alert.silence.edit",
  SilenceDelete = "api.alert.silence.delete",
  NotificationChannelCreate = "api.alert.notification.create",
  NotificationChannelList = "api.alert.notification.list",
  NotificationChannelPublish = "api.alert.notification.publish",
  NotificationChannelTest = "api.alert.notification.test",
  NotificationChannelUpdate = "api.alert.notification.update",
  NotificationChannelRetire = "api.alert.notification.retire",
  NotificationChannelRead = "api.alert.notification.get",
  DatasetCreate = "api.datasets.create",
  DatasetUpdate = "api.datasets.update",
  DatasetRead = "api.datasets.read",
  DatasetList = "api.datasets.list",
  DataIngest = "api.data.in",
  DataOut = "api.data.out",
  DataExhaust = "api.data.exhaust",
  QueryTemplateCreate = "api.query.template.create",
  QueryTemplateRead = "api.query.template.read",
  QueryTemplateDelete = "api.query.template.delete",
  QueryTemplateList = "api.query.template.list",
  QueryTemplateUpdate = "api.query.template.update",
  QueryTemplate = "api.query.template.query",
  SchemaValidator = "api.schema.validator",
  GenerateSignedUrl = "api.files.generate-url",
  DatasetStatusTransition = "api.datasets.status-transition",
  DatasetHealth = "api.dataset.health",
  DatasetReset = "api.dataset.reset",
  DatasetSchemaGenerator = "api.datasets.dataschema",
  DatasetExport = "api.datasets.export",
  DatasetCopy = "api.datasets.copy",
  ConnectorList = "api.connectors.list",
  ConnectorRead = "api.connectors.read",
  DatasetImport = "api.datasets.import",
  SQLQuery = "api.obsrv.data.sql-query",
}

interface AccessControl {
  [key: string]: string[];
}

const accessControl: AccessControl = {
  [roles.Ingestor]: [permissions.DataIngest],
  [roles.Viewer]: [
    permissions.DatasetList,
    permissions.DatasetRead,
    permissions.DatasetExport,
    permissions.ConnectorRead,
    permissions.SQLQuery,
    permissions.DataOut,
    permissions.DataExhaust,
    permissions.AlertList,
    permissions.AlertDetails,
    permissions.MetricList,
    permissions.SilenceList,
    permissions.SilenceRead,
    permissions.NotificationChannelList,
    permissions.NotificationChannelRead,
  ],
  [roles.DatasetCreator]: [
    permissions.DatasetList,
    permissions.DatasetRead,
    permissions.DatasetExport,
    permissions.ConnectorRead,
    permissions.SQLQuery,
    permissions.DataOut,
    permissions.DataExhaust,
    permissions.DatasetImport,
    permissions.DatasetCreate,
    permissions.DatasetUpdate,
    permissions.DatasetCopy,
    permissions.QueryTemplateCreate,
    permissions.QueryTemplateRead,
    permissions.QueryTemplateDelete,
    permissions.QueryTemplateUpdate,
    permissions.GenerateSignedUrl,
    permissions.SchemaValidator,
    permissions.DatasetSchemaGenerator,
    permissions.AlertList,
    permissions.AlertDetails,
    permissions.MetricList,
    permissions.SilenceList,
    permissions.SilenceRead,
    permissions.NotificationChannelList,
    permissions.NotificationChannelRead,
  ],
  [roles.DatasetManager]: [
    permissions.DatasetList,
    permissions.DatasetRead,
    permissions.DatasetExport,
    permissions.ConnectorRead,
    permissions.SQLQuery,
    permissions.DataOut,
    permissions.DataExhaust,
    permissions.DatasetImport,
    permissions.DatasetCreate,
    permissions.DatasetUpdate,
    permissions.DatasetCopy,
    permissions.QueryTemplateCreate,
    permissions.QueryTemplateRead,
    permissions.QueryTemplateDelete,
    permissions.QueryTemplateUpdate,
    permissions.GenerateSignedUrl,
    permissions.SchemaValidator,
    permissions.DatasetSchemaGenerator,
    permissions.DatasetReset,
    permissions.DatasetStatusTransition,
    permissions.AlertList,
    permissions.AlertDetails,
    permissions.MetricList,
    permissions.SilenceList,
    permissions.SilenceRead,
    permissions.NotificationChannelList,
    permissions.NotificationChannelRead,
  ],
  [roles.Admin]: [
    permissions.DatasetCreate,
    permissions.DatasetList,
    permissions.DatasetRead,
    permissions.DatasetExport,
    permissions.ConnectorRead,
    permissions.SQLQuery,
    permissions.DataOut,
    permissions.DataExhaust,
    permissions.DatasetImport,
    permissions.DatasetCreate,
    permissions.DatasetUpdate,
    permissions.DatasetCopy,
    permissions.QueryTemplateCreate,
    permissions.QueryTemplateRead,
    permissions.QueryTemplateDelete,
    permissions.QueryTemplateUpdate,
    permissions.GenerateSignedUrl,
    permissions.SchemaValidator,
    permissions.DatasetSchemaGenerator,
    permissions.DatasetReset,
    permissions.DatasetStatusTransition,
    permissions.AlertList,
    permissions.AlertDetails,
    permissions.MetricList,
    permissions.SilenceList,
    permissions.SilenceRead,
    permissions.NotificationChannelList,
    permissions.NotificationChannelRead,
  ],
  [roles.OperationsAdmin]: [
    permissions.AlertCreate,
    permissions.AlertPublish,
    permissions.AlertUpdate,
    permissions.AlertList,
    permissions.AlertDelete,
    permissions.AlertDetails,
    permissions.MetricCreate,
    permissions.MetricList,
    permissions.MetricUpdate,
    permissions.MetricRemove,
    permissions.SilenceCreate,
    permissions.SilenceList,
    permissions.SilenceRead,
    permissions.SilenceEdit,
    permissions.SilenceDelete,
    permissions.NotificationChannelCreate,
    permissions.NotificationChannelList,
    permissions.NotificationChannelPublish,
    permissions.NotificationChannelTest,
    permissions.NotificationChannelUpdate,
    permissions.NotificationChannelRetire,
    permissions.NotificationChannelRead,
    permissions.DatasetList,
    permissions.DatasetRead,
    permissions.DatasetExport,
    permissions.ConnectorRead,
    permissions.SQLQuery,
    permissions.DataOut,
    permissions.DataExhaust,
  ],
};

export default {
  name: "rbac:middleware",
  handler: () => (req: Request, res: Response, next: NextFunction) => {
    try {
      if (_.lowerCase(config.is_RBAC_enabled) === "false") {
        next();
      } else {
        const public_key = config.user_token_public_key;
        const token = req.get("x-user-token");
        if (!token) {
          return ResponseHandler.errorResponse(
            {
              statusCode: 403,
              errCode: "FORBIDDEN",
              message: "No token provided",
            },
            req,
            res
          );
        }
        jwt.verify(token as string, public_key, (err, decoded) => {
          if (err) {
            return ResponseHandler.errorResponse(
              {
                statusCode: 403,
                errCode: "FORBIDDEN",
                message: "Token verification failed",
              },
              req,
              res
            );
          }
          if (decoded && _.isObject(decoded)) {
            (req as any).userInfo = decoded;
            const action = (req as any).id;
            const hasAccess = decoded?.roles?.some(
              (role: string) =>
                accessControl[role] && accessControl[role].includes(action)
            );
            if (!hasAccess) {
              return ResponseHandler.errorResponse(
                {
                  statusCode: 401,
                  errCode: "Unauthorized access",
                  message: "Access denied for the user",
                },
                req,
                res
              );
            }
            next();
          }
        });
      }
    } catch (error) {
      next(error);
    }
  },
};
