import { AxiosInstance } from "axios";
import { NextFunction, Request, Response } from "express";
import _ from "lodash";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { ErrorResponseHandler } from "../helpers/ErrorResponseHandler";
import { IConnector } from "../models/DatasetModels";
import { DbConnector } from "../connectors/DbConnector";
import { KafkaConnector } from "../connectors/KafkaConnector";




export class HealthService {
  private dbConnector: DbConnector;
  private kafkaConnector: KafkaConnector;
  private httpDruidConnector: AxiosInstance;
  private errorHandler: ErrorResponseHandler;
  constructor(dbConnector: DbConnector, kafkaConnector: KafkaConnector, httpDruidConnector: IConnector) {
    this.errorHandler = new ErrorResponseHandler("HealthService");
    this.httpDruidConnector = httpDruidConnector.connect()
    this.dbConnector = dbConnector;
    this.kafkaConnector = kafkaConnector;
  }

  checkHealth(req: Request, res: Response, next: NextFunction) {
    Promise.all([this.checkDruidHealth(), this.checkKafkaHealth(), this.checkPostgresHealth()])
    .then(() => {
      ResponseHandler.successResponse(req, res, { status: 200, data: {} })
    }).catch(error => {
      this.errorHandler.handleError(req, res, next, error)
    })
  }

  private async checkDruidHealth() {
    await this.httpDruidConnector.get("/status/health")
  }

  private async checkKafkaHealth() {
    await this.kafkaConnector.connect()
  }

  private async checkPostgresHealth() {
    await this.dbConnector.health()
  }
  
}
