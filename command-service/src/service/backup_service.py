import os
import subprocess
import tarfile
import time

import boto3

# from google.cloud import storage as gcs_storage
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


class BackupService:
    def __init__(self, pod_name, namespace="default"):
        self.pod_name = pod_name
        self.namespace = namespace

    def compress_file(self, file_path):
        timestamp = time.strftime("%Y%m%d%H%M%S")
        tar_file_path = f"{file_path}_{timestamp}.tar.gz"
        with tarfile.open(tar_file_path, "w:gz") as tar:
            tar.add(file_path, arcname=os.path.basename(file_path))
        return tar_file_path

    def compress_remote_path(self, remote_path):
        timestamp = time.strftime("%Y%m%d%H%M%S")
        tar_file_path = f"{remote_path}_{timestamp}.tar.gz"
        compress_command = f"tar -czvf {tar_file_path} {os.path.dirname(remote_path)}/{os.path.basename(remote_path)}"
        cmd = [
            "kubectl",
            "exec",
            self.pod_name,
            "-n",
            self.namespace,
            "--",
            "sh",
            "-c",
            compress_command,
        ]
        subprocess.run(cmd, check=True)
        return tar_file_path

    def copy_to_pod(self, local_file_path, remote_file_path):
        cmd = [
            "kubectl",
            "cp",
            local_file_path,
            f"{self.pod_name}:{remote_file_path}",
            "-n",
            self.namespace,
        ]
        subprocess.run(cmd, check=True)

    def copy_from_pod(self, remote_file_path, local_file_path):
        cmd = [
            "kubectl",
            "cp",
            f"{self.pod_name}:{remote_file_path}",
            local_file_path,
            "-n",
            self.namespace,
        ]
        subprocess.run(cmd, check=True)

    def upload_to_s3(self, file_path, bucket_name, object_name):
        s3_client = boto3.client("s3")
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{object_name}")

    def cleanup_remote(self, remote_file_path):
        cmd = [
            "kubectl",
            "exec",
            self.pod_name,
            "-n",
            self.namespace,
            "--",
            "rm",
            "-rf",
            remote_file_path,
        ]
        subprocess.run(cmd, check=True)

    def upload_to_gcs(self, file_path, bucket_name, object_name):
        pass

    #     client = gcs_storage.Client()
    #     bucket = client.bucket(bucket_name)
    #     blob = bucket.blob(object_name)
    #     blob.upload_from_filename(file_path)
    #     print(f"Uploaded {file_path} to gs://{bucket_name}/{object_name}")

    def upload_to_azure(self, file_path, connection_string, container_name, blob_name):
        pass

    #     blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    #     blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    #     with open(file_path, "rb") as data:
    #         blob_client.upload_blob(data)
    #     print(f"Uploaded {file_path} to azure://{container_name}/{blob_name}")

    def process_snapshot(self, remote_path, storage_type, storage_config):
        remote_file_path = self.compress_remote_path(remote_path)
        tar_file_path = os.path.basename(remote_file_path)
        try:
            self.copy_from_pod(remote_file_path, tar_file_path)
            destination_path = f"{storage_config['backup_prefix']}/{os.path.basename(remote_file_path)}"

            if storage_type == "s3":
                self.upload_to_s3(
                    tar_file_path, storage_config["bucket_name"], destination_path
                )
            # elif storage_type == 'gcs':
            #     self.upload_to_gcs(tar_file_path, storage_config['bucket_name'], destination_path)
            # elif storage_type == 'azure':
            #     self.upload_to_azure(tar_file_path, storage_config['connection_string'], storage_config['container_name'], destination_path)
            else:
                raise ValueError(
                    "Unsupported storage type. Choose 's3', 'gcs', or 'azure'."
                )
        finally:
            if os.path.exists(tar_file_path):
                os.remove(tar_file_path)
            self.cleanup_remote(remote_file_path)


# Example usage:
# if __name__ == "__main__":
#     uploader = SnapshotUploader(
#         pod_name='my-pod', # Replace with your pod name
#         namespace='default'  # Replace with your namespace
#     )

#     file_path = '/path/to/local/directory_or_file'
#     remote_path = '/path/in/pod/directory_or_file.tar.gz'
#     storage_type = 's3'  # or 'gcs' or 'azure'
#     storage_config = {
#         'bucket_name': 'your-s3-bucket-name',
#         'backup_prefix': 'your-s3-backup-prefix'
#     }
#     # For GCS
#     # storage_config = {
#     #     'bucket_name': 'your-gcs-bucket-name',
#     #     'backup_prefix': 'your-gcs-backup-prefix'
#     # }
#     # For Azure
#     # storage_config = {
#     #     'connection_string': 'your-azure-connection-string',
#     #     'container_name': 'your-container-name',
#     #     'backup_prefix': 'your-azure-backup-prefix'
#     # }

#     uploader.process_snapshot(file_path, remote_path, storage_type, storage_config)
