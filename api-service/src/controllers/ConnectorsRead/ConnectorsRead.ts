import { Request, Response } from "express";
import { obsrvError } from "../../types/ObsrvError";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import { connectorService } from "../../services/ConnectorService";

const defaultFields = ["id", "connector_id", "name", "type", "category", "version", "description", "licence", "owner", "iconurl", "status", "ui_spec", "created_by", "updated_by", "created_date", "updated_date", "live_date"];

const validateRequest = (req: Request) => {

    const { id } = req.params;
    const mode = req.query.mode;

    if (mode && mode !== "edit") {
        throw obsrvError(id, "DATASET_INVALID_MODE_VALUE", `The specified mode [${mode}] in the query param is not valid.`, "BAD_REQUEST", 400);
    }

}

const connectorsRead = async (req: Request, res: Response) => {
    validateRequest(req)
    const { id } = req.params;
    const { mode } = req.query;
    const connector = (mode == "edit") ? await readDraftConnector(id, defaultFields) : await readConnector(id, defaultFields)
    if (!connector) {
        throw obsrvError(id, "CONNECTOR_NOT_FOUND", `Connector with the given id: ${id} not found`, "NOT_FOUND", 404);
    } else {
        ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: connector });
    }
}

const readDraftConnector = async (Id: string, defaultFields: string[]): Promise<any> => {
    const connector = await connectorService.getDraftConnector(Id, "Draft", defaultFields);
    return connector;
}

const readConnector = async (Id: string, defaultFields: string[]): Promise<any> => {
    const connector = await connectorService.getConnector(Id, defaultFields);
    return connector;
}

export default connectorsRead;