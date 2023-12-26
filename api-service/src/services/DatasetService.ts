import { Request, Response, NextFunction } from "express";
import _ from 'lodash'
import { Datasets } from "../helpers/Datasets";
import { findAndSetExistingRecord, updateTelemetryAuditEvent } from "./telemetry";
import { DbUtil } from "../helpers/DbUtil";
import { refreshDatasetConfigs } from "../helpers/DatasetConfigs";
import { ErrorResponseHandler } from "../helpers/ErrorResponseHandler";
import { DatasetStatus, IConnector } from "../models/DatasetModels";

const telemetryObject = { id: null, type: "dataset", ver: "1.0.0" };

export class DatasetService {
    private table: string
    private dbConnector: IConnector;
    private dbUtil: DbUtil;
    private errorHandler: ErrorResponseHandler;
    constructor(dbConnector: IConnector, table: string) {
        this.dbConnector = dbConnector
        this.table = table
        this.dbUtil = new DbUtil(dbConnector, table)
        this.errorHandler = new ErrorResponseHandler("DatasetService");
    }

    public save = async (req: Request, res: Response, next: NextFunction) => {
        try {
            const dataset = new Datasets(req.body)
            const payload: any = dataset.setValues()
            updateTelemetryAuditEvent({ request: req, object: { ...telemetryObject, id: _.get(payload, 'dataset_id') } });
            await this.dbUtil.save(req, res, next, payload)
        } catch (error: any) { this.errorHandler.handleError(req, res, next, error) }
    }

    public update = async (req: Request, res: Response, next: NextFunction) => {
        try {
            const dataset = new Datasets(req.body)
            const payload = dataset.getValues()
            await findAndSetExistingRecord({ dbConnector: this.dbConnector, table: this.table, request: req, filters: { "id": payload.id }, object: { ...telemetryObject, id: payload.id } });
            await this.dbUtil.update(req, res, next, payload)
            await refreshDatasetConfigs()
        } catch (error: any) { this.errorHandler.handleError(req, res, next, error) }
    }

    public read = async (req: Request, res: Response, next: NextFunction) => {
        try {
            let status: any = req.query.status || DatasetStatus.Live
            const id = req.params.datasetId
            updateTelemetryAuditEvent({ request: req, object: { ...telemetryObject, id } });
            await this.dbUtil.read(req, res, next, { id, status })
        } catch (error: any) { this.errorHandler.handleError(req, res, next, error, false) }
    }

    public list = async (req: Request, res: Response, next: NextFunction) => {
        try {
            const payload = req.body
            await this.dbUtil.list(req, res, next, payload)
        } catch (error: any) { this.errorHandler.handleError(req, res, next, error, false) }
    }
}
