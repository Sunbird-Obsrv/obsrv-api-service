import winston from "winston";

 winston.configure({
    level: "info",
    transports: [new winston.transports.Console()],
});


const logger = {
    error: (...data: any) => console.error("error", data),
    info: (...data: any) => console.log("info", data),
    warn: (...data: any) => console.log("warn", data),
}

export default logger;