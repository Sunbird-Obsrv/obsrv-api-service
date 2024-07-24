import time

from command.dataset_command import DatasetCommand
from command.icommand import ICommand
from model.data_models import Action, ActionResponse, CommandPayload, DatasetStatusType
from model.telemetry_models import Audit, Object, Property, Transition
from service.telemetry_service import TelemetryService


class TelemetryCommand(ICommand):
    def __init__(
        self, telemetry_service: TelemetryService, dataset_command: DatasetCommand
    ):
        self.dataset_command = dataset_command
        self.telemetry_service = telemetry_service

    def execute(self, command_payload: CommandPayload, action: Action, ts: int):
        if action == Action.CREATE_AUDIT_EVENT.name:
            print(
                f"Invoking CREATE_AUDIT_EVENT command for dataset_id {command_payload.dataset_id}..."
            )
            object_, audit, error = self._create_audit_event(command_payload, ts)
            if error:
                return error
            self.telemetry_service.audit(object_=object_, edata=audit)
            return ActionResponse(status="OK", status_code=200)

    def _create_audit_event(self, command_payload: CommandPayload, ts: int):
        dataset_id = command_payload.dataset_id
        dataset_record = self.dataset_command._get_draft_dataset_record(dataset_id)
        if dataset_record:
            object_ = Object(
                dataset_id, dataset_record["type"], dataset_record["max_version"]
            )
            draft_property = Property(
                "draft-dataset:status",
                DatasetStatusType.ReadyToPublish.name,
                DatasetStatusType.Live.name,
            )
            dataset_property = Property(
                "dataset:status",
                DatasetStatusType.ReadyToPublish.name,
                DatasetStatusType.Live.name,
            )
            transition = Transition(duration=int(time.time() - ts))
            edata = Audit(
                props=[draft_property, dataset_property], transition=transition
            )
            return object_, edata, None
        else:
            return (
                None,
                None,
                ActionResponse(
                    status="ERROR",
                    status_code=404,
                    error_message="DATASET_ID_NOT_FOUND",
                ),
            )
