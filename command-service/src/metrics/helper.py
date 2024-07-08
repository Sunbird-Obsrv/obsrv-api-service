import time

from .metrics import Metrics


class Helper:

    def __init__(self, registry):
        self.start_time = time.time()
        self.metrics = Metrics(registry)

    def get_duration(self, start_time):
        duration = int(time.time() * 1000) - start_time if start_time else None
        return duration

    def onRequest(self, entity, id, endpoint, dataset_id):
        self.start_time = int(time.time() * 1000)
        self.metrics.totalApiCallsMetric().labels(
            entity=entity, id=id, endpoint=endpoint, datasetId=dataset_id
        ).inc()

    def onFailedRequest(self, entity, id, endpoint, dataset_id, status):
        duration = self.get_duration(self.start_time)
        self.metrics.queryResponseTimeMetric().labels(
            entity=entity, id=id, endpoint=endpoint, datasetId=dataset_id, status=status
        ).set(duration)
        self.metrics.failedApiCallsMetric().labels(
            entity=entity, id=id, endpoint=endpoint, datasetId=dataset_id, status=status
        ).inc()

    def onSuccessRequest(self, entity, id, endpoint, dataset_id):
        duration = self.get_duration(self.start_time)
        self.metrics.queryResponseTimeMetric().labels(
            entity=entity, id=id, endpoint=endpoint, datasetId=dataset_id, status=200
        ).set(duration)
        self.metrics.successApiCallsMetric().labels(
            entity=entity, id=id, endpoint=endpoint, datasetId=dataset_id, status=200
        ).inc()
