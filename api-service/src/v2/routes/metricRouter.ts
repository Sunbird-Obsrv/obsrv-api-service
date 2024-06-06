import express from "express";
import { metricsScrapeHandler } from "../metrics/prometheus";

export const router = express.Router();
//Scrape metrics to prometheus
router.get("/metrics", metricsScrapeHandler)