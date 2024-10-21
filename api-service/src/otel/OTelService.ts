import { Counter, diag, DiagConsoleLogger, DiagLogLevel, Meter, metrics } from '@opentelemetry/api';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { LoggerProvider, BatchLogRecordProcessor } from '@opentelemetry/sdk-logs';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import logger from '../logger';
import * as logsAPI from '@opentelemetry/api-logs';


export class OTelService {
    private static meterProvider: MeterProvider;
    private static loggerProvider: LoggerProvider;
    private static tracerProvider: NodeTracerProvider;

    public static init() {
        const collectorEndpoint = process.env.OTEL_COLLECTOR_ENDPOINT || 'http://localhost:4318';
        this.tracerProvider = this.createTracerProvider(collectorEndpoint); // Store the tracer provider
        this.meterProvider = this.createMeterProvider(collectorEndpoint); // Store the meter provider
        this.loggerProvider = this.createLoggerProvider(collectorEndpoint); // Store the logger provider

        // Register the global tracer, meter, and logger providers
        this.tracerProvider.register();
        this.setGlobalMeterProvider(this.meterProvider);

        logger.info("OpenTelemetry Service Initialized");

        // Add shutdown hook
        process.on('SIGTERM', async () => {
            await this.tracerProvider.shutdown();
            await this.meterProvider.shutdown();
            await this.loggerProvider.shutdown();
        });
    }

    private static createTracerProvider(endpoint: string) {
        const traceExporter = new OTLPTraceExporter({
            url: `${endpoint}/v1/traces`,
        });

        const tracerProvider = new NodeTracerProvider({
            resource: this.createServiceResource('obsrv-api-service'),
        });

        tracerProvider.addSpanProcessor(new BatchSpanProcessor(traceExporter));

        return tracerProvider;
    }

    private static createMeterProvider(endpoint: string) {
        const metricExporter = new OTLPMetricExporter({
            url: `${endpoint}/v1/metrics`,
        });

        const meterProvider = new MeterProvider({
            resource: this.createServiceResource('obsrv-api-service'),
        });

        meterProvider.addMetricReader(
            new PeriodicExportingMetricReader({
                exporter: metricExporter,
                exportIntervalMillis: 10000,
            })
        );

        return meterProvider;
    }

    private static createLoggerProvider(endpoint: string) {
        const logExporter = new OTLPLogExporter({
            url: `${endpoint}/v1/logs`,
        });

        const loggerProvider = new LoggerProvider({
            resource: this.createServiceResource('obsrv-api-service'),
        });

        loggerProvider.addLogRecordProcessor(
            new BatchLogRecordProcessor(logExporter)
        );

        return loggerProvider;
    }

    // Helper method to create a Resource with service name
    private static createServiceResource(serviceName: string) {
        return new Resource({
            [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
        });
    }

    private static setGlobalMeterProvider(meterProvider: MeterProvider) {
        diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);
        diag.info('Registering MeterProvider globally.');
        metrics.setGlobalMeterProvider(meterProvider);
    }

    // Method to create a counter metric
    public static createCounterMetric(name: string): Counter {
        const meter = this.getMeterProvider(); // Use the updated getMeterProvider method
        const counter = meter.createCounter(name, {
            description: 'Counts the number of API calls',
        });
        return counter;
    }

    public static getMeterProvider(): Meter {
        return this.meterProvider.getMeter('obsrv-api-service');
    }

    public static getLoggerProvider(): LoggerProvider {
        return this.loggerProvider;
    }

    public static getTracerProvider(): NodeTracerProvider {
        return this.tracerProvider;
    }

    // Method to record the counter metric
    public static recordCounter(counter: Counter, value: number) {
        counter.add(value, {
            // Optional attributes can be added here
            service: 'obsrv-api-service',
        });
    }

    // Method to log messages
    public static log() {
        const loggerInstance = this.loggerProvider.getLogger('obsrv-api-service'); // Retrieve a logger instance

        loggerInstance.emit({
            severityNumber: logsAPI.SeverityNumber.INFO,
            severityText: 'INFO',
            body: 'test',
            attributes: { 'log.type': 'LogRecord' },
        });
    }

    public static emitAuditLog(auditLog: Record<string, any>) {
        const loggerInstance = this.loggerProvider.getLogger('obsrv-api-service');
    
        // Construct the log record
        const logRecord = {
            severityNumber: logsAPI.SeverityNumber.INFO, // or ERROR depending on the context
            severityText: 'INFO',
            body: JSON.stringify(auditLog), // Convert the log object to a string
            attributes: {
                'log.type': 'AuditLog',
                ...auditLog, // Include the whole log object as attributes if necessary
            },
        };
    
        // Emit the log record to OpenTelemetry
        loggerInstance.emit(logRecord);
    
        // Log the same message to Winston (optional)
        logger.info("Audit log emitted", { auditLog });
    }
}
