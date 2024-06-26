import { Request, Response } from "express";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import _ from "lodash";
import logger from "../../logger";
import { config } from "../../configs/Config";
import axios from "axios";
import httpStatus from "http-status";
import busboy from "busboy";
import { v4 } from "uuid"
import { PassThrough } from "stream";
import { URLAccess } from "../../types/SampleURLModel";
import { ErrorObject } from "../../types/ResponseModel";

export const apiId = "api.connector.stream.upload";
export const code = "FAILED_TO_REGISTER_CONNECTOR";

const apiServiceHost = _.get(config, ["obsrv_api_service_config", "host"]);
const apiServicePort = _.get(config, ["obsrv_api_service_config", "port"]);
const generateSignedURLPath = _.get(config, ["obsrv_api_service_config", "generate_url_path"]);

const commandServiceHost = _.get(config, ["command_service_config", "host"]);
const commandServicePort = _.get(config, ["command_service_config", "port"]);
const registryUrl = _.get(config, ["command_service_config", "connector_registry_path"])

const getGenerateSignedURLRequestBody = (files: string[], access: string) => ({
    id: apiId,
    ver: "v2",
    ts: Date.now(),
    params: {
        msgid: v4()
    },
    request: {
        files,
        access: access || URLAccess.Read,
        type: "connector"
    }
});

const connectorRegistryStream = async (req: Request, res: Response) => {
    const resmsgid = _.get(res, "resmsgid");
    try {
        const uploadStreamResponse: any = await uploadStream(req);
        const registryRequestBody = {
            relative_path: uploadStreamResponse[0]
        }
        logger.info({ apiId, resmsgid, message: `File uploaded to cloud provider successfully` })
        const registryResponse = await axios.post(`${commandServiceHost}:${commandServicePort}${registryUrl}`, registryRequestBody);
        logger.info({ apiId, resmsgid, message: `Connector registered successfully` })
        ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: { message: registryResponse?.data?.message } })
    } catch (error: any) {
        const errMessage = _.get(error, "response.data.error.message")
        logger.error(error, apiId, resmsgid, code);
        let errorMessage = error;
        const statusCode = _.get(error, "statusCode")
        if (!statusCode || statusCode == 500) {
            errorMessage = { code, message: errMessage || "Failed to register connector" }
        }
        ResponseHandler.errorResponse(errorMessage, req, res);
    }
};

export const generatePresignedUrl = async (fileName: string, access: string) => {
    try {
        const requestBody = getGenerateSignedURLRequestBody([fileName], access);
        const response = await axios.post(`${apiServiceHost}:${apiServicePort}${generateSignedURLPath}`, requestBody);
        return response?.data?.result;
    }
    catch (err) {
        throw {
            code: "FILES_GENERATE_URL_FAILURE",
            message: "Failed to generate sample urls",
            statusCode: 400,
            errCode: "BAD_REQUEST"
        } as ErrorObject
    }
};

const uploadStream = async (req: Request) => {
    return new Promise((resolve, reject) => {
        const filePromises: Promise<void>[] = [];
        const bb = busboy({ headers: req.headers });
        const relative_path: any[] = [];
        let fileCount = 0;

        bb.on("file", async (name: any, file: any, info: any) => {
            if (fileCount > 0) {
                // If more than one file is detected, reject the request
                bb.emit("error", reject({
                    code: "FAILED_TO_UPLOAD",
                    message: "Uploading multiple files are not allowed",
                    statusCode: 400,
                    errCode: "BAD_REQUEST"
                }));
                return
            }
            fileCount++;
            const processFile = async () => {
                const fileName = info?.filename;
                const preSignedUrl: any = await generatePresignedUrl(fileName, URLAccess.Write);
                const filePath = preSignedUrl[0]?.filePath
                const fileNameExtracted = extractFileNameFromPath(filePath);
                relative_path.push(...fileNameExtracted);
                const pass = new PassThrough();
                file.pipe(pass);
                const fileBuffer = await streamToBuffer(pass);
                await axios.put(preSignedUrl[0]?.preSignedUrl, fileBuffer, {
                    headers: {
                        "Content-Type": info.mimeType,
                        "Content-Length": fileBuffer.length,
                    }
                });
            };
            filePromises.push(processFile());
        });
        bb.on("close", async () => {
            try {
                await Promise.all(filePromises);
                resolve(relative_path);
            } catch (error) {
                reject({
                    code: "FAILED_TO_UPLOAD",
                    message: "Fail to upload a file",
                    statusCode: 400,
                    errCode: "BAD_REQUEST"
                });
            }
        });
        bb.on("error", reject);
        req.pipe(bb);
    })
}

const streamToBuffer = (stream: PassThrough): Promise<Buffer> => {
    return new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("end", () => resolve(Buffer.concat(chunks)));
        stream.on("error", reject);
    });
};

const extractFileNameFromPath = (filePath: string): string[] => {
    const regex = /(?<=\/)[^/]+\.[^/]+(?=\/|$)/g;
    return filePath.match(regex) || [];
};

export default connectorRegistryStream;
