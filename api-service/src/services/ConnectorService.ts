import { ConnectorRegistry } from "../models/ConnectorRegistry";

class ConnectorService {

    findConnectors = async (where?: Record<string, any>, attributes?: string[]): Promise<any> => {
        return ConnectorRegistry.findAll({ where, attributes, raw: true });
    }

    getConnector = async (Id: string, attributes?: string[]): Promise<any> => {
        return ConnectorRegistry.findOne({ where: { id: Id }, attributes });
    }

    getDraftConnector = async (Id: string, Status: string, attributes?: string[]): Promise<any> => {
        return ConnectorRegistry.findOne({ where: { id: Id, status: Status }, attributes });
    }

}

export const connectorService = new ConnectorService();