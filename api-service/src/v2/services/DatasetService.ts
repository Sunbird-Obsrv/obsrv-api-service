import _ from "lodash";
import logger from "../logger";
import { cipherService } from "./CipherService";
import { defaultDatasetConfig } from "../configs/DatasetConfigDefault";
import { Dataset } from "../models/Dataset";
import { DatasetDraft } from "../models/DatasetDraft";
import { DatasetTransformations } from "../models/Transformation";
import { DatasetTransformationsDraft } from "../models/TransformationDraft";


class DatasetService {

    getDataset = async (datasetId: string, raw = false): Promise<any> => {
        return Dataset.findOne({ where: { id: datasetId }, raw: raw });
    }

    getDuplicateDenormKey = (denormConfig: Record<string, any>): Array<string> => {
        if (denormConfig && _.isArray(_.get(denormConfig, "denorm_fields"))) {
            const denormFields = _.get(denormConfig, "denorm_fields")
            const denormOutKeys = _.map(denormFields, field => _.get(field, "denorm_out_field"))
            const duplicateDenormKeys: Array<string> = _.filter(denormOutKeys, (item: string, index: number) => _.indexOf(denormOutKeys, item) !== index);
            return duplicateDenormKeys;
        }
        return []
    }

    checkDatasetExists = async (dataset_id: string): Promise<boolean> => {
        const draft = await DatasetDraft.findOne({ where: { dataset_id }, attributes:["id"], raw: true });
        if (draft === null) {
            const live = await Dataset.findOne({ where: { id: dataset_id }, attributes:["id"], raw: true });
            return !(live === null)
        } else {
            return true;
        }
    }

    getDraftDataset = async (dataset_id: string) => {
        return DatasetDraft.findOne({ where: { dataset_id }, raw: true });
    }

    getDraftTransformations = async (dataset_id: string) => {
        return DatasetTransformationsDraft.findAll({ where: { dataset_id }, raw: true });
    }

    getTransformations = async (dataset_id: string) => {
        return DatasetTransformations.findAll({ where: { dataset_id }, raw: true });
    }

    updateDraftDataset = async (draftDataset: Record<string, any>): Promise<Record<string, any>> => {

        await DatasetDraft.update(draftDataset, { where: { id: draftDataset.id }})
        const responseData = { message: "Dataset is updated successfully", id: draftDataset.id, version_key: draftDataset.version_key }
        logger.info({ draftDataset, message: `Dataset updated successfully with id:${draftDataset.id}`, response: responseData })
        return responseData;
    }

    createDraftDataset = async (draftDataset: Record<string, any>): Promise<Record<string, any>> => {

        const response = await DatasetDraft.create(draftDataset)
        const responseData = { id: _.get(response, ["dataValues", "id"]) || "", version_key: draftDataset.version_key }
        logger.info({ draftDataset, message: `Dataset Created Successfully with id:${_.get(response, ["dataValues", "id"])}`, response: responseData })
        return responseData
    }

    

}

export const datasetService = new DatasetService();