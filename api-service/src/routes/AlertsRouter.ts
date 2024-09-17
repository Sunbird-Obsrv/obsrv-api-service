import express from "express";
import notificationHandler from "../controllers/NotificationChannel/Notification";
import { setDataToRequestObject } from "../middlewares/setDataToRequestObject";
import customAlertHandler from "../controllers/Alerts/Alerts";
import metricAliasHandler from "../controllers/Alerts/Metric";
import silenceHandler from "../controllers/Alerts/Silence";
import checkRBAC from "../middlewares/RBAC_middleware";

export const alertsRouter = express.Router();

// Notifications

alertsRouter.post("/notifications/search", setDataToRequestObject("api.alert.notification.list"), checkRBAC.handler(), notificationHandler.listHandler);
alertsRouter.post("/notifications/create", setDataToRequestObject("api.alert.notification.create"), checkRBAC.handler(), notificationHandler.createHandler);
alertsRouter.get("/notifications/publish/:id", setDataToRequestObject("api.alert.notification.publish"), checkRBAC.handler(), notificationHandler.publishHandler);
alertsRouter.post("/notifications/test", setDataToRequestObject("api.alert.notification.test"), checkRBAC.handler(), notificationHandler.testNotifationChannelHandler);
alertsRouter.patch("/notifications/update/:id", setDataToRequestObject("api.alert.notification.update"), checkRBAC.handler(), notificationHandler.updateHandler);
alertsRouter.delete("/notifications/delete/:id", setDataToRequestObject("api.alert.notification.retire"), checkRBAC.handler(), notificationHandler.retireHandler);
alertsRouter.get("/notifications/get/:id", setDataToRequestObject("api.alert.notification.get"), checkRBAC.handler(), notificationHandler.fetchHandler);

// alerts
alertsRouter.post("/create", setDataToRequestObject("api.alert.create"), checkRBAC.handler(), customAlertHandler.createAlertHandler);
alertsRouter.get("/publish/:alertId", setDataToRequestObject("api.alert.publish"), checkRBAC.handler(), customAlertHandler.publishAlertHandler);
alertsRouter.post(`/search`, setDataToRequestObject("api.alert.list"), checkRBAC.handler(), customAlertHandler.searchAlertHandler);
alertsRouter.get("/get/:alertId", setDataToRequestObject("api.alert.getAlertDetails"), checkRBAC.handler(), customAlertHandler.alertDetailsHandler);
alertsRouter.delete("/delete/:alertId", setDataToRequestObject("api.alert.delete"), checkRBAC.handler(), customAlertHandler.deleteAlertHandler);
alertsRouter.delete("/delete", setDataToRequestObject("api.alert.delete"), checkRBAC.handler(), customAlertHandler.deleteSystemAlertsHandler);
alertsRouter.patch("/update/:alertId", setDataToRequestObject("api.alert.update"), checkRBAC.handler(), customAlertHandler.updateAlertHandler);

// metrics
alertsRouter.post("/metric/alias/create",setDataToRequestObject("api.metric.add"), checkRBAC.handler(), metricAliasHandler.createMetricHandler);
alertsRouter.post("/metric/alias/search", setDataToRequestObject("api.metric.list"), checkRBAC.handler(), metricAliasHandler.listMetricsHandler);
alertsRouter.patch("/metric/alias/update/:id", setDataToRequestObject("api.metric.update"), checkRBAC.handler(), metricAliasHandler.updateMetricHandler);
alertsRouter.delete("/metric/alias/delete/:id", setDataToRequestObject("api.metric.remove"), checkRBAC.handler(), metricAliasHandler.deleteMetricHandler);
alertsRouter.delete("/metric/alias/delete", setDataToRequestObject("api.metric.remove"), checkRBAC.handler(), metricAliasHandler.deleteMultipleMetricHandler);

// silence
alertsRouter.post("/silence/create",setDataToRequestObject("api.alert.silence.create"), checkRBAC.handler(), silenceHandler.createHandler);
alertsRouter.get("/silence/search",setDataToRequestObject("api.alert.silence.list"), checkRBAC.handler(), silenceHandler.listHandler);
alertsRouter.get("/silence/get/:id",setDataToRequestObject("api.alert.silence.get"), checkRBAC.handler(), silenceHandler.fetchHandler);
alertsRouter.patch("/silence/update/:id",setDataToRequestObject("api.alert.silence.edit"), checkRBAC.handler(), silenceHandler.updateHandler);
alertsRouter.delete("/silence/delete/:id",setDataToRequestObject("api.alert.silence.delete"), checkRBAC.handler(), silenceHandler.deleteHandler);