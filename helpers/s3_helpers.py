import boto3
import botocore
from datetime import datetime
import logging
from io import BytesIO
import os
import pandas as pd
from typing import List, TypedDict, Optional
from helpers.settings import Settings

class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]

class S3Client:
    def __init__(self):
        self.region = Settings.SCW_S3_REGION
        self.url = Settings.SCW_S3_URL
        self.access_key = Settings.SCW_ACCESS_KEY
        self.secret_key = Settings.SCW_SECRET_KEY
        self.bucket = Settings.SCW_S3_BUCKET

        self.client = boto3.client(
            service_name="s3",
            region_name=self.region,
            use_ssl=True,
            endpoint_url=self.url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def get_root_dirpath(self) -> str:
        return f"etl/{Settings.AIRFLOW_ENV}"

    def upload_file(self, key: str, body: str | bytes, acl: str = "private"):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ACL=acl)

    def download_file(self, key: str):
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def load_csv_from_s3(self, object_storage_key: str, sep: str = ";"):
        downloaded_file = self.download_file(object_storage_key)
        file_content = BytesIO(downloaded_file)
        df = pd.read_csv(file_content, sep=sep, encoding="utf-8", index_col=False)
        return df

    def send_files(self, list_files: List[File]):
        for file in list_files:
            is_file = os.path.isfile(os.path.join(file["source_path"], file["source_name"]))
            logging.info(f"Sending {file['source_name']}")
            if is_file:
                with open(os.path.join(file["source_path"], file["source_name"]), "rb") as f:
                    self.upload_file(
                        f"{self.get_root_dirpath()}/{file['dest_path']}{file['dest_name']}",
                        f.read(),
                        acl="private"
                    )
            else:
                raise Exception(f"File {file['source_path']}{file['source_name']} does not exist")

    def get_files_from_prefix(self, prefix: str):
        list_objects = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{self.get_root_dirpath()}/{prefix}"):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):  # Exclude folders
                    list_objects.append(obj["Key"].replace(f"{self.get_root_dirpath()}/", ""))
        return list_objects

    def get_files(self, list_files: List[File]):
        for file in list_files:
            self.client.download_file(
                self.bucket,
                f"{self.get_root_dirpath()}/{file['source_path']}{file['source_name']}",
                f"{file['dest_path']}{file['dest_name']}"
            )

    def get_latest_file_s3(self, s3_path: str, local_path: str):
        objects = self.client.list_objects_v2(Bucket=self.bucket, Prefix=s3_path)
        db_files = [obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".gz")]

        sorted_db_files = sorted(
            db_files,
            key=lambda x: datetime.strptime(x.split("_")[-1].split(".")[0], "%Y-%m-%d"),
            reverse=True,
        )

        if sorted_db_files:
            latest_db_file = sorted_db_files[0]
            logging.info(f"Latest dirigeants database: {latest_db_file}")
            self.client.download_file(self.bucket, latest_db_file, local_path)
        else:
            logging.warning("No .gz files found in the specified path.")

    def delete_file(self, file_path: str):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=file_path)
            logging.info(f"File '{file_path}' deleted successfully.")
        except botocore.exceptions.ClientError as e:
            logging.error(e)

    def get_files_and_last_modified(self, prefix: str):
        file_info_list = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                file_info_list.append((obj["Key"], obj["LastModified"]))
        logging.info(f"List of files: {file_info_list}")
        return file_info_list

    def compare_files(self, file_path_1: str, file_path_2: str, file_name_1: str, file_name_2: str):
        try:
            file_1 = self.client.head_object(Bucket=self.bucket, Key=f"{self.get_root_dirpath()}/{file_path_1}{file_name_1}")
            file_2 = self.client.head_object(Bucket=self.bucket, Key=f"{self.get_root_dirpath()}/{file_path_2}{file_name_2}")

            logging.info(f"Hash file 1: {file_1['ETag']}")
            logging.info(f"Hash file 2: {file_2['ETag']}")

            return file_1["ETag"] == file_2["ETag"]
        except botocore.exceptions.ClientError as e:
            logging.error(f"Error loading files: {e}")
            return None

    def rename_folder(self, old_folder_suffix: str, new_folder_suffix: str):
        old_folder = f"{self.get_root_dirpath()}/{old_folder_suffix}"
        new_folder = f"{self.get_root_dirpath()}/{new_folder_suffix}"

        if not old_folder.endswith("/"):
            old_folder += "/"
        if not new_folder.endswith("/"):
            new_folder += "/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=old_folder):
            for obj in page.get("Contents", []):
                old_key = obj["Key"]
                new_key = old_key.replace(old_folder, new_folder, 1)

                self.client.copy_object(
                    Bucket=self.bucket,
                    CopySource={"Bucket": self.bucket, "Key": old_key},
                    Key=new_key
                )
                logging.info(f"Copied {old_key} to {new_key}")

                self.client.delete_object(Bucket=self.bucket, Key=old_key)
                logging.info(f"Deleted {old_key}")

        logging.info(f"Folder '{old_folder}' renamed to '{new_folder}'")

    def download_folder(self, folder_name: str, local_dir: str):
        paginator = self.client.get_paginator("list_objects_v2")
        result_iterator = paginator.paginate(Bucket=self.bucket, Prefix=folder_name)

        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        for page in result_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                relative_path = os.path.relpath(key, folder_name)
                local_file_path = os.path.join(local_dir, relative_path)

                if key.endswith("/"):
                    continue

                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                self.client.download_file(self.bucket, key, local_file_path)

        print(f"Folder '{folder_name}' downloaded successfully to '{local_dir}'")

    def make_folder_public(self, folder_name: str):
        paginator = self.client.get_paginator("list_objects_v2")
        result_iterator = paginator.paginate(Bucket=self.bucket, Prefix=folder_name)

        for page in result_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                self.client.put_object_acl(Bucket=self.bucket, Key=key, ACL="public-read")

s3_client = S3Client()
