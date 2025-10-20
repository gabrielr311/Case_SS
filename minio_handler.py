from minio import Minio
from minio.error import S3Error
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig
import logging
from io import BytesIO
from typing import Optional, Tuple
from file_operations import create_put_obj_metadata
from formats import DocumentTypes, DataSources, CVMDocumentAggregationType, BucketCustomMetadata, ContentTypes
from pathlib import Path

class MinioHandler:
    def __init__(self,logger: logging.Logger, bucket_name: str,host: str, port: str, username: str, password: str):
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.logger = logger

        # Initialize the MinIO client
        self.client = Minio(
            f"{self.host}:{self.port}",
            access_key=self.username, 
            secret_key=self.password, 
            secure=False
        )
        try:
            self.client.list_buckets()
        except S3Error as e:
            self.logger.error(f"Authentication error while trying to create MinIO client. Please double check the credentials passed. Error message: {e}")
            exit(1)

        self.logger.info("Authenticated to MinIO with success")

        try:
            if not self.client.bucket_exists(self.bucket_name):

                self.logger.info(f"Desired bucket '{self.bucket_name}' does not exists, creating bucket...")
                self.create_and_configure_bucket()
                self.logger.info("Bucket created and configured successfully.")

        except S3Error as e:
            self.logger.error(f"Error occurred while trying to create the bucket with name {self.bucket_name}: {e}")
            exit(1)

    def create_and_configure_bucket(self):

        self.client.make_bucket(self.bucket_name)
        # Enable versioning (ENABLED). Use SUSPENDED to suspend.
        self.logger.info(f"Bucket '{self.bucket_name}' created with success")
        self.logger.info("Configuring bucket`s versioning...")
        self.client.set_bucket_versioning(self.bucket_name, VersioningConfig(ENABLED))
        cfg = self.client.get_bucket_versioning(self.bucket_name)
        # cfg is a VersioningConfig-like object — check .status or print raw
        status = getattr(cfg, "status", None)

        print(f"Versioning status for '{self.bucket_name}': {status}")

        self.logger.info("Versioning configuration done with success")

        self.logger.info("Implementing medallion layout in created bucket...")

        # Medallion prefixes to ensure they appear in console/UI
        prefixes = [
            "bronze/raw/",
            "bronze/landing/",
            "silver/cleaned/",
            "silver/enriched/",
            "gold/export/",
            "gold/serving/",
            "gold/documents/",
            "logs/"
        ]

        for p in prefixes:
            placeholder_key = p.rstrip("/") + "/.placeholder"
            try:
                # small 1-byte object (some engines dislike zero-length)
                self.client.put_object(
                    bucket_name=self.bucket_name,
                    object_name=placeholder_key,
                    data=BytesIO(b" "),  # single space
                    length=1,
                    content_type="application/octet-stream",
                )
                self.logger.info(f"Created placeholder: s3://{self.bucket_name}/{placeholder_key}")
            except S3Error as e:
                self.logger.error(f"Failed to create placeholder {placeholder_key}: {e}")

        self.logger.info(f"Medallion pattern layout created with success")


    def get_file_bytes_and_metadata(self,file_path: str) -> Tuple[BytesIO,dict[str,str]]:
        """
        Gets the object content from MinIO`s get_object() and the metadata from stat_object()

        """

        file_from_bucket = self.client.get_object(self.bucket_name,file_path)
        file_bytes = BytesIO(file_from_bucket.read())

        file_metadata = self.client.stat_object(self.bucket_name,file_path).metadata

        return file_bytes,file_metadata
    
    def save_file_to_bucket(self,
                            save_file_path : str,
                            file_bytes: BytesIO,
                            ingest_ts: str,
                            trace_id: str,
                            doc_type : DocumentTypes,
                            content_type: ContentTypes,
                            data_source: DataSources,
                            ref_date : Optional[str] = None,
                            agg_type : Optional[CVMDocumentAggregationType] = None):
        
        """
        Function to create a file metadata and save it to a bucket
                Logs go to both terminal and the log file configured in the passed logger instance.

        Args:
            save_file_path (str)                                   : Full bucket file path to be saved.
            file_bytes (BytesIO)                                   : BytesIO representing the file to be saved
            ingest_ts (str)                                        : Timestamp to be used as the ingestion timestamp.
            trace_id (str)                                         : Ingestion job trace ID
            save_file_path (str)                                   : Full bucket file path to be saved.
            doc_type (fmts.DocumentTypes)                          : Document type to be used in the metadata.
            content_type (ContentTypes)                            : File content-type from fmts.ContentTypes
            data_source (fmts.DataSources)                         : File data source as defined in fmts.DataSources
            ref_date: (Optional[str])                              : File reference date
            agg_type : (Optional[fmts.CVMDocumentAggregationType]) : If inserting a CVM file, the aggregation type as defined in fmts.CVMDocumentAggregationType
            
        """

        ref_date = ref_date if ref_date else ""

        metadata = create_put_obj_metadata(
                                            ingest_ts=ingest_ts,
                                            ref_date=ref_date,
                                            trace_id=trace_id,
                                            source=data_source,
                                            document_type=doc_type,
                                            file_bytes=file_bytes,
                                            aggregation_type=agg_type
                                        )
        
        new_file_hash = metadata["file_hash"]

        parent_folder = Path(save_file_path).parent

        list_obj_ret = self.client.list_objects(
                                                bucket_name=self.bucket_name,
                                                prefix=f"{parent_folder}/",
                                                recursive = True
                                            )
        
        file_paths = [obj.object_name for obj in list_obj_ret if not obj.object_name.endswith("/")]

        for curr_file_in_parent_folder in file_paths:

            curr_file_metadata = self.client.stat_object(self.bucket_name,curr_file_in_parent_folder).metadata

            if new_file_hash == curr_file_metadata.get(BucketCustomMetadata.FILE_HASH.value,None):
                self.logger.warning(f"The file trying to be uploaded already exists as of path '{curr_file_in_parent_folder}'. Aborting upload...")
                return

        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=save_file_path,
            data=file_bytes,  
            length=len(file_bytes.getvalue()),
            content_type=content_type.value,
            metadata=metadata)
        
        self.logger.info(f"File uplodaded with success to '{save_file_path}'")
    
        
        
        
