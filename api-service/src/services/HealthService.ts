import { Request, Response } from "express"
import { ResponseHandler } from "../helpers/ResponseHandler"

class HealthService {
  
  async checkDruidHealth(req: Request, res: Response) {
    ResponseHandler.successResponse(req, res, { status: 200, data: {} })
  }

  async checkKafkaHealth() : Promise<boolean> {
    return true
  }

  async checkPostgresHealth() : Promise<boolean> {
    return true
  }
  
}

export const healthService = new HealthService()