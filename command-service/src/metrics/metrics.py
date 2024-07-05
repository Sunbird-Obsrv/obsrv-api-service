from prometheus_client import Counter, Gauge


class Metrics:
    def __init__(self, registry) -> None:
        self.node_query_response_time = Gauge(
            name="node_query_response_time",
            documentation="The average response time for database queries",
            labelnames=["entity", "id", "endpoint", "datasetId", "status"],
            registry=registry,
        )
        self.total_api_calls = Counter(
            name="node_total_api_calls",
            documentation="The total number of API calls made",
            labelnames=["entity", "id", "endpoint", "datasetId"],
            registry=registry,
        )
        self.failed_api_calls = Counter(
            name="node_failed_api_calls",
            documentation="The total number of failed API calls made",
            labelnames=["entity", "id", "endpoint", "datasetId", "status"],
            registry=registry,
        )
        self.success_api_calls = Counter(
            name="node_success_api_calls",
            documentation="The total number of successful API calls made",
            labelnames=["entity", "id", "endpoint", "datasetId", "status"],
            registry=registry,
        )

    def queryResponseTimeMetric(self):
        return self.node_query_response_time

    def totalApiCallsMetric(self):
        return self.total_api_calls

    def failedApiCallsMetric(self):
        return self.failed_api_calls

    def successApiCallsMetric(self):
        return self.success_api_calls
