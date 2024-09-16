import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { config } from "../configs/Config";
import _ from "lodash";
import userPermissions from "./userPermissions.json";
interface AccessControl {
  apiGroups : {
    [key: string]: string[];
  },
  roles : {
    [key: string]: string[];
  }
}

const accessControl: AccessControl = userPermissions;

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
              statusCode: 401,
              errCode: "Unauthorized access",
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
            if (!decoded?.id) {
              return ResponseHandler.errorResponse(
                {
                  statusCode: 401,
                  errCode: "Unauthorized access",
                  message: "User ID is missing from the decoded token.",
                },
                req,
                res
              );
            }
            (req as any).userID = decoded?.id;
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
                  statusCode: 403,
                  errCode: "FORBIDDEN",
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
