import { Datasource } from "../models/Datasource";

export const getDatasourceList = async (datasetId: string, raw = false) => {
    const dataSource = await Datasource.findAll({
        where: {
            dataset_id: datasetId,
        },
        raw: raw
    });
    return dataSource
}












