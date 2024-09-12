import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { config } from "../configs/Config";
import _ from "lodash";

interface AccessControl {
  apiGroups : {
    [key: string]: string[];
  },
  roles : {
    [key: string]: string[];
  }
}

const accessControl: AccessControl = {
  "apiGroups": {
      "general_access": [
          "api.datasets.list",
          "api.datasets.read",
          "api.datasets.export",
          "api.data.out",
          "api.data.exhaust",
          "api.alert.list",
          "api.alert.getAlertDetails",
          "api.metric.list",
          "api.alert.silence.list",
          "api.alert.silence.get",
          "api.alert.notification.list",
          "api.alert.notification.get"
      ],
      "restricted_dataset_api": [
          "api.datasets.reset",
          "api.datasets.status-transition"
      ],
      "alert": [
          "api.alert.create",
          "api.alert.publish",
          "api.alert.update",
          "api.alert.delete"
      ],
      "metric": [
          "api.metric.add",
          "api.metric.update",
          "api.metric.remove"
      ],
      "silence": [
          "api.alert.silence.create",
          "api.alert.silence.edit",
          "api.alert.silence.delete"
      ],
      "notificationChannel": [
          "api.alert.notification.create",
          "api.alert.notification.publish",
          "api.alert.notification.test",
          "api.alert.notification.update",
          "api.alert.notification.retire"
      ],
      "dataset": [
          "api.datasets.create",
          "api.datasets.update",
          "api.datasets.import",
          "api.datasets.copy",
          "api.dataset.health",
          "api.datasets.dataschema"
      ],
      "data": [
          "api.data.in"
      ],
      "queryTemplate": [
          "api.query.template.create",
          "api.query.template.read",
          "api.query.template.delete",
          "api.query.template.update",
          "api.query.template.query",
          "api.query.template.list"
      ],
      "schema": [
          "api.schema.validator"
      ],
      "file": [
          "api.files.generate-url"
      ],
      "connector": [
          "api.connectors.list",
          "api.connectors.read"
      ],
      "sqlQuery": [
          "api.obsrv.data.sql-query"
      ]
  },
  "roles": {
      "ingestor": [
          "data"
      ],
      "viewer": [
          "general_access",
          "connector",
          "sqlQuery"
      ],
      "dataset_creator": [
          "general_access",
          "connector",
          "sqlQuery",
          "dataset",
          "queryTemplate",
          "schema",
          "file",
          "connector",
          "sqlQuery"
      ],
      "dataset_manager": [
          "general_access",
          "connector",
          "sqlQuery",
          "dataset",
          "queryTemplate",
          "schema",
          "file",
          "connector",
          "sqlQuery",
          "restricted_dataset_api"
      ],
      "admin": [
          "general_access",
          "connector",
          "sqlQuery",
          "dataset",
          "queryTemplate",
          "schema",
          "file",
          "connector",
          "sqlQuery",
          "restricted_dataset_api"
      ],
      "operations_admin": [
          "alert",
          "metric",
          "silence",
          "notificationChannel",
          "general_access"
      ]
  }
}

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
            const hasAccess = decoded?.roles?.some((role: string) => {
              const apiGroups = accessControl.roles[role];

              if (!apiGroups) return false;

              return apiGroups.some((apiGroup: string) =>
                accessControl.apiGroups[apiGroup]?.includes(action)
              );
            });
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
