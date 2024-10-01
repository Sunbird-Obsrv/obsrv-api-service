import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { config } from "../configs/Config";
import _ from "lodash";
import userPermissions from "./userPermissions.json";
import httpStatus from "http-status";
import { userService } from "../services/UserService";
interface AccessControl {
  apiGroups: {
    [key: string]: string[];
  },
  roles: {
    [key: string]: string[];
  }
}

const accessControl: AccessControl = userPermissions;

const errorHandler = (statusCode: number, message: string, req: Request, res: Response) => {
  const errorMapping: Record<number, { errCode: string, code: string }> = {
    401: {
      errCode: httpStatus["401_NAME"],
      code: "UNAUTHORIZED ACCESS",
    },
    403: {
      errCode: httpStatus["403_NAME"],
      code: "FORBIDDEN ACCESS",
    },
  };

  const { errCode, code } = errorMapping[statusCode];

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

const authType = config.authenticationType;

export default {
  name: "rbac:middleware",
  handler: () => async (req: Request, res: Response, next: NextFunction) => {
    try {
      if (_.lowerCase(config.is_RBAC_enabled) === "false") {
        (req as any).userID = "SYSTEM";
        next();
      } else if (authType === "basic") {
        const public_key = config.user_token_public_key;
        const token = req.get("x-user-token");
        if (!token) {
          return errorHandler(401, "No token provided", req, res);
        }
        jwt.verify(token as string, public_key, (err, decoded) => {
          if (err) {
            return errorHandler(401, "Token verification failed", req, res);
          }
          if (decoded && _.isObject(decoded)) {
            if (!decoded?.id) {
              return errorHandler(401, "User ID is missing from the decoded token.", req, res);
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
                ? `The following roles have access to this action: ${rolesWithAccess.join(", ")}`
                : "No roles have this action";

              const errorMessage = `Access denied. User does not have permission to perform this action. ${rolesMessage}.`;

              return errorHandler(403, errorMessage, req, res);
            }
            next();
          }
        });
      }
      else if (authType === "keycloak") {
        const token = req.get("x-user-token");

        if (!token) {
          return res.status(401).json({ message: "No token provided" });
        }
        const decoded = jwt.decode(token);
        if (!decoded) {
          return res.status(401).json({ message: "Invalid token" });
        }
        const userCondition: any = {};
        userCondition.id = decoded.sub;
        const userDetails = ["roles", "user_name"]
        const user = await userService.getUser(userCondition, userDetails);

        if (!user) {
          return res.status(404).json({ message: "User not found" });
        }

        (req as any).userID = decoded?.sub;
        const action = (req as any).id;
        const hasAccess = user?.roles?.some((role: string) => {
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
            ? `The following roles have access to this action: ${rolesWithAccess.join(", ")}`
            : "No roles have this action";

          const errorMessage = `Access denied. User does not have permission to perform this action. ${rolesMessage}.`;

          return errorHandler(403, errorMessage, req, res);
        }
        next();
      }
    } catch (error) {
      next(error);
    }
  },
};
