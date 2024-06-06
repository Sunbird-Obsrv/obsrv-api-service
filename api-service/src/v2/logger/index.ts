import winston from "winston";

 winston.configure({
    level: "info",
    transports: [new winston.transports.Console()],
});

const innerLogger = new (winston.Logger)();
const logger = {
    error: (...data: any) => innerLogger.error("", data),
    info: (...data: any) => innerLogger.info("", data),
    warn: (...data: any) => innerLogger.warn("", data),
}

export default logger;