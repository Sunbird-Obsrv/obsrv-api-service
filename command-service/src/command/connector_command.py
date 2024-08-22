from dacite import from_dict
import json
import logging
import subprocess

from command.icommand import ICommand
from config import Config
from model.data_models import Action, ActionResponse, CommandPayload, DatasetStatusType
from model.db_models import ConnectorInstance
from service.db_service import DatabaseService


class ConnectorCommand(ICommand):
    def __init__(self, config: Config, db_service: DatabaseService):
        self.config = config
        self.db_service = db_service
        self.logger = logging.getLogger()
        self.connector_job_config = self.config.find("connector_jobs")

    def execute(self, command_payload: CommandPayload, action: Action):
        result = None
        active_connectors = self._get_connector_details(
            dataset_id=command_payload.dataset_id
        )
        is_masterdata = self._get_masterdata_details(
            dataset_id=command_payload.dataset_id
        )

        print(f"Active connectors: {active_connectors}")
        print(f"Is masterdata: {is_masterdata}")

        if action == Action.DEPLOY_CONNECTORS.name:
            result = self._deploy_connectors(
                command_payload.dataset_id, active_connectors, is_masterdata
            )

        return result

    def _deploy_connectors(self, dataset_id, active_connectors, is_masterdata):
        result = None
        namespaces = set()
        for job_type, config in self.connector_job_config.items():
            namespaces.add(config["namespace"])
        for namespace in namespaces:    
            self._stop_connector_jobs(dataset_id, active_connectors, is_masterdata, namespace)
        result = self._install_jobs(dataset_id, active_connectors, is_masterdata)

        return result

    def _stop_connector_jobs(self, dataset_id, active_connectors, is_masterdata, namespace):
        managed_releases = []
        connector_jar_config = self.config.find("connector_job")
        masterdata_jar_config = self.config.find("masterdata_job")
        for connector_type in connector_jar_config:
            for release in connector_jar_config[connector_type]:
                managed_releases.append(release["release_name"])
        if is_masterdata:
            for release in masterdata_jar_config:
                managed_releases.append(release["release_name"])

        helm_ls_cmd = ["helm", "ls", "--namespace", namespace]
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

            if connector.connector_runtime == "spark":
                result = self._perform_spark_install(dataset_id, connector)
            elif connector.connector_runtime == "flink":
                result = self._perform_flink_install(dataset_id, connector)
            else:
                print(
                    f"Connector {connector.connector_id} is not supported for deployment"
                )
                break
            
        # if is_masterdata:
        #     print("Installing masterdata job")
        #     masterdata_jar_config = self.config.find("masterdata_job")
        #     for release in masterdata_jar_config:
        #         result = self._perform_install(release)
        return result
    
    def _perform_flink_install(self, dataset_id, connector_instance):
        err = None
        result = None
        release_name = connector_instance.connector_id
        runtime = connector_instance.connector_runtime
        namespace = self.connector_job_config["flink"]["namespace"]
        
        helm_ls_cmd = ["helm", "ls", "--namespace", namespace]
        
        helm_ls_result = subprocess.run(
            helm_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        
        if helm_ls_result.returncode == 0:
            jobs = helm_ls_result.stdout.decode()
            print(jobs)
            deployment_exists = any(release_name in line for line in jobs.splitlines()[1:])
            if deployment_exists:
                restart_cmd = f"kubectl delete pods --selector app.kubernetes.io/name=flink,component={release_name}-jobmanager --namespace {namespace} && kubectl delete pods --selector app.kubernetes.io/name=flink,component={release_name}-taskmanager --namespace {namespace}".format(
                    namespace=namespace, release_name=release_name
                )
                print("Restart command: ", restart_cmd)
                # Run the helm command
                helm_install_result = subprocess.run(
                    restart_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,
                )
                if helm_install_result.returncode == 0:
                    print(f"Job {release_name} re-deployment succeeded...")
                else:
                    err = True
                    return ActionResponse(
                        status="ERROR",
                        status_code=500,
                        error_message="FLINK_HELM_LIST_EXCEPTION",
                    )
                    print(f"Error restarting pod: {helm_ls_result.stderr.decode()}")
                    
                if err is None:
                        result = ActionResponse(status="OK", status_code=200)

                return result
            else:
                connector_source = json.loads(connector_instance.connector_source)
                flink_jobs = {
                    "kafka-connector": {
                        "enabled": "true",
                        "job_classname": connector_source.get('main_class')
                    }
                }
                set_json_value = json.dumps(flink_jobs)
                print("Kafka connector: ", set_json_value)
                helm_install_cmd = [
                    "helm",
                    "upgrade",
                    "--install",
                    release_name,
                    f"""{self.config.find("helm_charts_base_dir")}/{self.connector_job_config["flink"]["base_helm_chart"]}""",
                    "--namespace",
                    namespace,
                    "--create-namespace",
                    "--set-json",
                    f"flink_jobs={set_json_value}"
                ]
                
                print(" ".join(helm_install_cmd))

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
                        error_message="FLINK_CONNECTOR_HELM_INSTALLATION_EXCEPTION",
                    )
                    print(
                        f"Error re-installing job {release_name}: {helm_install_result.stderr.decode()}"
                    )

                if err is None:
                    result = ActionResponse(status="OK", status_code=200)

                return result
        else:
            print(f"Error checking Flink deployments: {helm_ls_result.stderr.decode()}")
            return ActionResponse(
                status="ERROR",
                status_code=500,
                error_message="FLINK_HELM_LIST_EXCEPTION",
            )
            
    def _perform_spark_install(self, dataset_id, connector_instance):
        err = None
        result = None
        release_name = connector_instance.id
        connector_source = json.loads(connector_instance.connector_source)
        schedule = connector_instance.operations_config["schedule"]
        
        schedule_configs = {
            "Hourly": "0 * * * *",     # Runs at the start of every hour
            "Weekly": "0 0 * * 0",     # Runs at midnight every Sunday
            "Monthly": "0 0 1 * *",    # Runs at midnight on the 1st day of every month
            "Yearly": "0 0 1 1 *"      # Runs at midnight on January 1st each year
        }

        # Define namespace
        namespace = self.connector_job_config["spark"]["namespace"]
        
        # Check if the job already exists
        helm_ls_cmd = ["helm", "ls", "--namespace", namespace]
        helm_ls_result = subprocess.run(helm_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if helm_ls_result.returncode == 0:
            jobs = helm_ls_result.stdout.decode()
            job_exists = any(release_name in line for line in jobs.splitlines()[1:])
            
            if job_exists:
                if self._is_dataset_retired(dataset_id):
                    print("Dataset is retired. Uninstalling existing cron job.")
                    self._stop_connector_jobs(dataset_id, [], False, namespace)
                else:
                    print("Updating existing job")
                    
                    helm_install_cmd = [
                        "helm",
                        "upgrade",
                        "--install",
                        release_name,
                        f"""{self.config.find("helm_charts_base_dir")}/{self.connector_job_config["spark"]["base_helm_chart"]}""",
                        "--namespace",
                        namespace,
                        "--create-namespace",
                        "--set",
                        "technology={}".format(connector_instance.technology),
                        "--set",
                        "instance_id={}".format(release_name),
                        "--set",
                        "connector_source={}".format(connector_source["source"]),
                        "--set",
                        "main_class={}".format(connector_source["main_class"]),
                        "--set",
                        "main_file={}".format(connector_source["main_program"]),
                        "--set",
                        "cronSchedule={}".format(schedule_configs[schedule])
                    ]

                    print(" ".join(helm_install_cmd))

                    helm_install_result = subprocess.run(
                        helm_install_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    )
                    if helm_install_result.returncode == 0:
                        print(f"Job {release_name} update succeeded...")
                        result = ActionResponse(status="OK", status_code=200)
                    else:
                        err = True
                        result = ActionResponse(
                            status="ERROR",
                            status_code=500,
                            error_message="SPARK_CRON_HELM_INSTALLATION_EXCEPTION",
                        )
                        print(f"Error updating job {release_name}: {helm_install_result.stderr.decode()}")
            else:
                print("Installing new job")
                
                helm_install_cmd = [
                    "helm",
                    "install",
                    release_name,
                    f"""{self.config.find("helm_charts_base_dir")}/{self.connector_job_config["spark"]["base_helm_chart"]}""",
                    "--namespace",
                    namespace,
                    "--create-namespace",
                    "--set",
                    "technology={}".format(connector_instance.technology),
                    "--set",
                    "instance_id={}".format(release_name),
                    "--set",
                    "connector_source={}".format(connector_source["source"]),
                    "--set",
                    "main_class={}".format(connector_source["main_class"]),
                    "--set",
                    "main_file={}".format(connector_source["main_program"]),
                    "--set",
                    "cronSchedule={}".format(schedule_configs[schedule])
                ]

                print(" ".join(helm_install_cmd))

                helm_install_result = subprocess.run(
                    helm_install_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                if helm_install_result.returncode == 0:
                    print(f"Job {release_name} installation succeeded...")
                    result = ActionResponse(status="OK", status_code=200)
                else:
                    err = True
                    result = ActionResponse(
                        status="ERROR",
                        status_code=500,
                        error_message="SPARK_CRON_HELM_INSTALLATION_EXCEPTION",
                    )
                    print(f"Error installing job {release_name}: {helm_install_result.stderr.decode()}")

        else:
            print(f"Error listing Spark jobs: {helm_ls_result.stderr.decode()}")
            result = ActionResponse(
                status="ERROR",
                status_code=500,
                error_message="SPARK_HELM_LIST_EXCEPTION",
            )
        
        if result is None:
            result = ActionResponse(status="ERROR", status_code=500, error_message="UNKNOWN_ERROR")

        return result

    def _get_connector_details(self, dataset_id):
        active_connectors = []
        query = f"""
            SELECT ci.id, ci.connector_id, ci.operations_config, cr.runtime as connector_runtime, cr.source as connector_source, cr.technology, cr.version
            FROM connector_instances ci
            JOIN connector_registry cr on ci.connector_id = cr.id
            WHERE ci.status= %s and ci.dataset_id = %s
        """
        params = (DatasetStatusType.Live.name, dataset_id,)
        records = self.db_service.execute_select_all(sql=query, params=params)

        for record in records:
            active_connectors.append(from_dict(
                data_class=ConnectorInstance, data=record
            ))
            # connector_versions[connector_instance.id] = record['version']

        return active_connectors

    def _get_masterdata_details(self, dataset_id):
        is_masterdata = False
        query = f"""
            SELECT *
            FROM datasets
            WHERE status= %s AND dataset_id = %s AND type = 'master'
        """
        params = (DatasetStatusType.Live.name, dataset_id,)
        rows = self.db_service.execute_select_all(sql=query, params=params)
        if len(rows) > 0:
            is_masterdata = True

        return is_masterdata
    
    def _is_dataset_retired(self, dataset_id):
        is_retired = False
        rows = self.db_service.execute_select_all(
            f"""
                SELECT status
                FROM datasets
                WHERE status='{DatasetStatusType.Retired.name}' AND dataset_id = '{dataset_id}'
            """
        )
        for row in rows:
            if row['status'] == DatasetStatusType.Retired.name:
                print("Status: ",row['status'])
                is_retired = True
                break
        
        return is_retired
    
    def _get_live_instances(self, dataset_id, runtime, connector_instance):
        active_connectors = []
        records = self.db_service.execute_select_all(
            f""" 
                SELECT d.id AS dataset_id, ci.id AS connector_instance_id, ci.connector_id
                FROM connector_instances ci
                JOIN connector_registry cr ON ci.connector_id = cr.id
                JOIN datasets d ON ci.dataset_id = d.id
                WHERE cr.runtime = '{runtime}' AND ci.status = '{DatasetStatusType.Live.name}';
            """
        )
        
        for record in records:
            active_connectors.append(record['connector_instance_id'])
            
        return active_connectors

