import { Request, Response } from "express";
import { obsrvError } from "../../types/ObsrvError";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import { connectorService } from "../../services/ConnectorService";

const defaultFields = ["id", "connector_id", "name", "type", "category", "version", "description", "licence", "owner", "iconurl", "status", "ui_spec", "created_by", "updated_by", "created_date", "updated_date", "live_date"];

const connectorsRead = async (req: Request, res: Response) => {
    const { id } = req.params;
    const { mode } = req.query;
    
    const connector = (mode?.toString()?.toLowerCase()) === "edit" ? await connectorService.getConnector({ id: id, status: "Draft" }, defaultFields) : await connectorService.getConnector({ id: id, status: "Live" }, defaultFields);
    if (!connector) {
        throw obsrvError(id, "CONNECTOR_NOT_FOUND", `Connector not found: ${id}`, "NOT_FOUND", 404);
    } else {
        ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: connector });
    }
}

export default connectorsRead;