import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import fs from "fs";
import { ResponseHandler } from "../helpers/ResponseHandler";
import _ from "lodash";

enum roles {
    Admin = "admin",
    DatasetManager = "dataset_manager",
    Viewer = "viewer",
    DatasetCreator = "dataset_creator",
}

enum permissions {
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
    SQLQuery = "api.obsrv.data.sql-query"
}

interface AccessControl {
    [key: string]: string[];
}

const accessControl: AccessControl = {
    [roles.Admin]: [
        permissions.DatasetRead,
        permissions.DatasetList,
        permissions.DatasetHealth,
        permissions.DatasetReset,
        permissions.QueryTemplateList,
        permissions.ConnectorRead,
        permissions.ConnectorList,
        permissions.SQLQuery,
    ],
    [roles.DatasetManager]: [
        permissions.DatasetUpdate,
        permissions.DatasetRead,
        permissions.DatasetList,
        permissions.DatasetHealth,
        permissions.DatasetReset,
        permissions.DatasetCopy,
        permissions.QueryTemplateRead,
        permissions.QueryTemplateUpdate,
        permissions.QueryTemplateDelete,
        permissions.QueryTemplateList,
        permissions.SchemaValidator,
        permissions.ConnectorRead,
        permissions.ConnectorList,
        permissions.SQLQuery,
    ],
    [roles.Viewer]: [
        permissions.DataOut,
        permissions.DataExhaust,
        permissions.DatasetRead,
        permissions.DatasetList,
        permissions.DatasetHealth,
        permissions.DatasetReset,
        permissions.QueryTemplateRead,
        permissions.QueryTemplateList,
        permissions.ConnectorRead,
        permissions.ConnectorList,
        permissions.SQLQuery,
    ],
    [roles.DatasetCreator]: [
        permissions.DatasetImport,
        permissions.DataIngest,
        permissions.DataOut,
        permissions.DatasetCreate,
        permissions.DatasetRead,
        permissions.DatasetUpdate,
        permissions.DatasetList,
        permissions.DatasetCopy,
        permissions.QueryTemplateCreate,
        permissions.QueryTemplateRead,
        permissions.QueryTemplateDelete,
        permissions.GenerateSignedUrl,
        permissions.SQLQuery,
        permissions.DatasetStatusTransition,
        permissions.DatasetSchemaGenerator,
    ],
};

export default {
  name: "jwt:tokenAuthorization",
  handler: () => (req: Request, res: Response, next: NextFunction) => {
    try {
      const public_key = fs.readFileSync("public_key.pem", "utf8");
      const authHeader = req.headers.authorization;
      const token = authHeader && authHeader.split(" ")[1];
      if (!token) {
        ResponseHandler.errorResponse(
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
            ResponseHandler.errorResponse(
                {
                  statusCode: 403,
                  errCode: "FORBIDDEN",
                  message: "Token verification failed",
                },
                req,
                res
              );
        }
        if (decoded && typeof decoded == "object") {
            const action = (req as any).id;
            const hasAccess = decoded.roles.some((role: string) => accessControl[role] && accessControl[role].includes(action));
          if (!hasAccess) {
            ResponseHandler.errorResponse(
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
    } catch (error) {
      next(error);
    }
  },
};
