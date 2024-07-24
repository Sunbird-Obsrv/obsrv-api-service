import logging
import subprocess

from command.icommand import ICommand
from config import Config
from model.data_models import Action, ActionResponse, CommandPayload
from service.http_service import HttpService


class FlinkCommand(ICommand):

    def __init__(self, config: Config, http_service: HttpService):
        self.config = config
        self.http_service = http_service
        self.logger = logging.getLogger()

    def execute(self, command_payload: CommandPayload, action: Action):
        result = None
        if action == Action.START_PIPELINE_JOBS.name:
            print(
                f"Invoking START_PIPELINE_JOBS command for dataset_id {command_payload.dataset_id}..."
            )
            result = self._restart_jobs()
        return result

    def _restart_jobs(self):
        return self._install_flink_jobs()

    def _restart_pods(self, release_name, namespace, job_name):
        restart_cmd = f"kubectl delete pods --selector app=flink,component={release_name}-jobmanager --namespace {namespace} && kubectl delete pods --selector app=flink,component={release_name}-taskmanager --namespace {namespace}".format(
            namespace=namespace, release_name=release_name
        )
        # Run the helm command
        helm_install_result = subprocess.run(
            restart_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        if helm_install_result.returncode == 0:
            print(f"Job {job_name} re-deployment succeeded...")
            return True
        else:
            print(
                f"Error re-installing job {job_name}: {helm_install_result.stderr.decode()}"
            )
            return False

    def _install_flink_jobs(self):
        result = ActionResponse(status="OK", status_code=200)
        flink_jobs = self.config.find("flink.jobs")
        namespace = self.config.find("flink.namespace")
        for job in flink_jobs:
            release_name = job["release_name"]
            job_name = job["name"]
            # Restart pods
            status = self._restart_pods(
                release_name=release_name, namespace=namespace, job_name=job_name
            )
            if not status:
                result = ActionResponse(
                    status="ERROR",
                    status_code=500,
                    error_message="FLINK_HELM_INSTALLATION_EXCEPTION",
                )
        return result
