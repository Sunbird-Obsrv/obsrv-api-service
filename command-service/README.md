# command-service
## Implementation

### Commands
* Each command implementation under the command module will extend from icommand interface and will have to implement the execute function. 
* The execute function will take a command payload json object and also an action as input. For e.g. The DruidCommand class' execute function will take the command payload and SUBMIT_INGESTION_TASK action as two parameters.
* Currently implemented command classes are DruidCommand, FlinkCommand and DbCommand classes.

### Services
Currently, there are two generic services under the services module:

* DatabaseService implements all the required database operations such as select_one, select_all and upsert operations from Postgresql. The service uses psycopg2 library to connect to Postgres.
* HttpService implements GET, POST and DELETE operations. The service uses urllib3 library to invoke http urls.

### Configuration

* The config.py class has an utility implementation `find` to read nested configurations from a yaml file. 
* The service_config.yml class has all the required configurations for the service.
    - The flink.jobs configuration is required to specify the list of jobs and the corresponding job_manager_urls. This is required for restarting the required jobs.
    - The commands entry will have the workflow of sub-commands for each higher level comamnd. For e.g., PUBLISH_DATASET command is comprised for five sub-commands such as MAKE_DATASET_LIVE, SUBMIT_INGESTION_TASKS, STOP_PIPELINE_JOBS and START_PIPELINE_JOBS.

### Deployment

```
helm install command-api ./helm-charts/command-service -n command-service
```
