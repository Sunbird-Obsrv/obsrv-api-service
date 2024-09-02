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
    Ingestor = "ingestor"
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
    [roles.Ingestor]: [
        permissions.DataIngest,
    ],
    [roles.Viewer]: [
        permissions.DatasetList,
        permissions.DatasetRead,
        permissions.DatasetExport,
        permissions.ConnectorRead,
        permissions.SQLQuery,
        permissions.DataOut,
        permissions.DataExhaust,
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
        permissions.GenerateSignedUrl,
        permissions.SchemaValidator,
        permissions.DatasetSchemaGenerator
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
        permissions.GenerateSignedUrl,
        permissions.SchemaValidator,
        permissions.DatasetSchemaGenerator,
        permissions.DatasetReset,
        permissions.DatasetStatusTransition
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
        permissions.GenerateSignedUrl,
        permissions.SchemaValidator,
        permissions.DatasetSchemaGenerator,
        permissions.DatasetReset,
        permissions.DatasetStatusTransition
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
