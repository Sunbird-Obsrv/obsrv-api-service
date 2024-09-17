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

const errorHandler = (statusCode: number, message: string, req: Request, res: Response) => {
  let errCode: string;
  let code: string;

  switch (statusCode) {
    case 401:
      errCode = "Unauthorized access";
      code = "UNAUTHORIZED ACCESS";
      break;
    case 403:
      errCode = "Forbidden access";
      code = "FORBIDDEN ACCESS";
      break;
    default:
      errCode = "Unknown error";
      code = "UNKNOWN ERROR";
  }

  return ResponseHandler.errorResponse(
    {
      statusCode,
      errCode,
      message,
      code,
    },
    req,
    res
  );
};

export default {
  name: "rbac:middleware",
  handler: () => (req: Request, res: Response, next: NextFunction) => {
    try {
      if (_.lowerCase(config.is_RBAC_enabled) === "false") {
        (req as any).userID = "SYSTEM";
        next();
      } else {
        const public_key = config.user_token_public_key;
        const token = req.get("x-user-token");
        if (!token) {
          errorHandler(401, "No token provided", req, res);
        }
        jwt.verify(token as string, public_key, (err, decoded) => {
          if (err) {
            errorHandler(401, "Token verification failed", req, res);
          }
          if (decoded && _.isObject(decoded)) {
            if (!decoded?.id) {
              errorHandler(401, "User ID is missing from the decoded token.", req, res);
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
              const rolesWithAccess = Object.keys(accessControl.roles).filter(role => {
                const apiGroups = accessControl.roles[role];
                if (!apiGroups) return false;
                return apiGroups.some(apiGroup => accessControl.apiGroups[apiGroup]?.includes(action));
              });
              const rolesMessage = rolesWithAccess.length > 0
                ? `Roles with access: ${rolesWithAccess.join(", ")}`
                : "No roles have this action";

              const errorMessage = `Access denied. User does not have permission to perform this action. ${rolesMessage}.`;

              errorHandler(403, errorMessage, req, res);
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
