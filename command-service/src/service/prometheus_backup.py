import requests

from config import Config
from service.backup_service import BackupService


class PrometheusBackup:
    def __init__(self) -> None:
        self.config = Config()
        self.prometheus_host = self.config.find("prometheus.host")
        self.prometheus_endpoint = self.config.find("prometheus.endpoint")
        self.prom_pod = self.config.find("prometheus.pod")
        self.prom_ns = self.config.find("prometheus.namespace")
        self.backup_provider = self.config.find("backups.provider")
        self.backup_bucket = self.config.find("backups.bucket")
        self.backup_prefix = self.config.find("prometheus.backup_prefix")

    def backup_prometheus(self) -> None:
        snapshot_uploader = BackupService(
            pod_name=self.prom_pod, namespace=self.prom_ns
        )

        # trigger prometheus snapshot
        snapshot_url = f"{self.prometheus_host}{self.prometheus_endpoint}"

        response = requests.post(snapshot_url)

        if response.status_code != 200 or response.json()["status"] != "success":
            raise Exception(f"Failed to trigger prometheus snapshot: {response.text}")

        snapshot_file_path = "/prometheus/snapshots"
        snapshot_id = response.json()["data"]["name"]

        snapshot_uploader.process_snapshot(
            remote_path=f"{snapshot_file_path}/{snapshot_id}",
            storage_type=self.backup_provider,
            storage_config={
                "bucket_name": self.backup_bucket,
                "backup_prefix": self.backup_prefix,
            },
        )

        snapshot_uploader.cleanup_remote(f"{snapshot_file_path}/{snapshot_id}")


if __name__ == "__main__":
    prometheus_backup = PrometheusBackup()
    prometheus_backup.backup_prometheus()
