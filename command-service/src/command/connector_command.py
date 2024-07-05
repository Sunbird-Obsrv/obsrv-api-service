import logging
import subprocess

from command.icommand import ICommand
from config import Config
from model.data_models import Action, ActionResponse, CommandPayload, DatasetStatusType
from service.db_service import DatabaseService


class ConnectorCommand(ICommand):
    def __init__(self, config: Config, db_service: DatabaseService):
        self.config = config
        self.db_service = db_service
        self.logger = logging.getLogger()
        self.connector_job_ns = "connector-jobs"
        self.connector_job_chart_dir = "{0}/connector-cron-jobs".format(
            self.config.find("helm_charts_base_dir")
        )

    def execute(self, command_payload: CommandPayload, action: Action):
        result = None
        active_connectors = self._get_connector_details(
            dataset_id=command_payload.dataset_id
        )
        is_masterdata = self._get_masterdata_details(
            dataset_id=command_payload.dataset_id
        )
        if action == Action.DEPLOY_CONNECTORS.name:
            result = self._deploy_connectors(
                command_payload.dataset_id, active_connectors, is_masterdata
            )

        return result

    def _deploy_connectors(self, dataset_id, active_connectors, is_masterdata):
        result = None
        self._stop_connector_jobs(dataset_id, active_connectors, is_masterdata)
        result = self._install_jobs(dataset_id, active_connectors, is_masterdata)

        return result

    def _stop_connector_jobs(self, dataset_id, active_connectors, is_masterdata):
        managed_releases = []
        connector_jar_config = self.config.find("connector_job")
        masterdata_jar_config = self.config.find("masterdata_job")
        for connector_type in connector_jar_config:
            for release in connector_jar_config[connector_type]:
                managed_releases.append(release["release_name"])
        if is_masterdata:
            for release in masterdata_jar_config:
                managed_releases.append(release["release_name"])

        helm_ls_cmd = ["helm", "ls", "--namespace", self.connector_job_ns]
        helm_ls_result = subprocess.run(
            helm_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        if helm_ls_result.returncode == 0:
            jobs = helm_ls_result.stdout.decode()
            for job in jobs.splitlines()[1:]:
                release_name = job.split()[0]
                if release_name in managed_releases:
                    print("Uninstalling job {0}".format(release_name))
                    helm_uninstall_cmd = [
                        "helm",
                        "uninstall",
                        release_name,
                        "--namespace",
                        self.connector_job_ns,
                    ]
                    helm_uninstall_result = subprocess.run(
                        helm_uninstall_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    if helm_uninstall_result.returncode == 0:
                        print(f"Successfully uninstalled job {release_name}...")
                    else:
                        print(
                            f"Error uninstalling job {release_name}: {helm_uninstall_result.stderr.decode()}"
                        )

    def _install_jobs(self, dataset_id, active_connectors, is_masterdata):
        result = None
        for connector in active_connectors:
            print("Installing connector {0}".format(connector))
            connector_jar_config = self.config.find("connector_job")[connector]
            for release in connector_jar_config:
                result = self._perform_install(release)
        if is_masterdata:
            print("Installing masterdata job")
            masterdata_jar_config = self.config.find("masterdata_job")
            for release in masterdata_jar_config:
                result = self._perform_install(release)
        return result

    def _perform_install(self, release):
        err = None
        result = None
        release_name = release["release_name"]
        helm_install_cmd = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            self.connector_job_chart_dir,
            "--namespace",
            self.connector_job_ns,
            "--create-namespace",
            "--set",
            "file.path={}".format(release["jar"]),
            "--set",
            "class.name={}".format(release["class"]),
            "--set",
            "job.name={}".format(release_name),
            "--set",
            "args={}".format(",".join(release["args"])),
            "--set",
            "schedule={}".format(release["schedule"]),
        ]
        helm_install_result = subprocess.run(
            helm_install_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if helm_install_result.returncode == 0:
            print(f"Job {release_name} deployment succeeded...")
        else:
            err = True
            result = ActionResponse(
                status="ERROR",
                status_code=500,
                error_message="FLINK_HELM_INSTALLATION_EXCEPTION",
            )
            print(
                f"Error re-installing job {release_name}: {helm_install_result.stderr.decode()}"
            )

        if err is None:
            result = ActionResponse(status="OK", status_code=200)

        return result

    def _get_connector_details(self, dataset_id):
        active_connectors = []
        rows = self.db_service.execute_select_all(
            f"""
                SELECT DISTINCT(connector_type)
                FROM dataset_source_config
                WHERE status='{DatasetStatusType.Live.name}' and connector_type IN ('object', 'jdbc')"""
        )

        for row in rows:
            active_connectors.append(row[0])

        return active_connectors

    def _get_masterdata_details(self, dataset_id):
        is_masterdata = False
        rows = self.db_service.execute_select_all(
            f"""
                SELECT *
                FROM datasets
                WHERE status='{DatasetStatusType.Live.name}' AND dataset_id = '{dataset_id}' AND type = 'master-dataset'"""
        )

        if len(rows) > 0:
            is_masterdata = True

        return is_masterdata
