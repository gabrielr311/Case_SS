
from datetime import datetime
from typing import List, Dict, Callable, Optional, Any, Iterable, Tuple
from collections import defaultdict
from pathlib import Path

import numpy as np
from ragflow_sdk import RAGFlow
from ragflow_sdk.modules.dataset import DataSet 
from ragflow_sdk.modules.document import Document 
from logging import Logger
import formats as fmts
from file_operations import PDFParser
import os

from io import BytesIO

class RagflowHandler:
    def __init__(self, 
                logger: Logger,
                base_url: str,
                api_key: str,
                embeddings_model_name : str
                ):
        
        self.logger = logger
        self.client = RAGFlow(base_url=base_url,api_key=api_key)
        self.pdf_parser = PDFParser(logger)
        self.embeddings_model_name = embeddings_model_name

    NAIVE_PARSER_CONFIG = {"chunk_count":512,
                            "delimiter":"\\n",
                            "html4excel":False,
                            "layout_recognize":True,
                            "raptor":{"use_raptor":False}
                        }

    def get_files_in_dataset(self,
                             file_name:str,
                             dataset_name : str) -> List[Document]:
        """
        Returns a list of tuples in the format (file_name,file_hash) for all files in a dataset.

        Args:
            ragflow_dataset.DataSet : The DataSet object to have its files listed

        Return:
            Dataset : Dataset where the file was found
            List[ragflow_sdk.modules.document.Document] : List of ragflow_sdk.modules.document.Document found by the search
        """

        try:

            dataset = self.client.list_datasets(name=dataset_name)[0]

        except Exception as e:
            error_msg = f"Dataset with name '{dataset_name}' doesnt exists."
            self.logger.error(error_msg)
            raise Exception(error_msg)

        return dataset,dataset.list_documents(keywords=file_name)

        
    def get_all_files_hash_in_dataset(self,dataset : DataSet):
        """
        Returns a list of tuples in the format (file_name,file_hash) for all files in a dataset.

        Args:
            ragflow_dataset.DataSet : The DataSet object to have its files listed

        Return:
            List[Tuple[str]] : List of tuples in the format (file_name,file_hash) for all files in a dataset
        """

        all_files_in_dataset = dataset.list_documents()

        file_names_and_hashes : List[Tuple[str]] = list()

        for curr_file in all_files_in_dataset:

            file_hash = curr_file.meta_fields.file_hash if hasattr(curr_file.meta_fields,"file_hash") else None

            file_names_and_hashes.append([curr_file.name,file_hash])

        return file_names_and_hashes

    def upload_document_and_start_parse(self,
                                        dataset_name: str,
                                        file_name: str,
                                        file_bytes: BytesIO,
                                        file_metadata : Dict[str,str]):
        
        treated_file_name = Path(file_name).name

        self.logger.info(f"Uploading file '{treated_file_name}' to RagFlow's dataset '{dataset_name}'...")

        try:

            dataset = self.client.list_datasets(name=dataset_name)[0]

        except Exception as e:
            self.logger.error("This dataset does not exists, please make sure it exists before uploading. Aborting...")

            exit(1)

        file_to_be_uploaded_hash = file_metadata[fmts.BucketCustomMetadata.FILE_HASH.value]

        for dataset_file_name,dataset_file_hash in self.get_all_files_hash_in_dataset(dataset):

            if file_to_be_uploaded_hash == dataset_file_hash:

                self.logger.warning(f"The file to be uploaded '{treated_file_name}' already exists in dataset '{dataset_name}' as '{dataset_file_name}'")

                return
            
        dataset.upload_documents([{"display_name": treated_file_name, "blob": file_bytes.getvalue()}])

        self.logger.info("File uploaded with success")

        self.logger.info("Checking if PDF file text is native...")

        document_meta_fields = defaultdict(dict)

        document_meta_fields["meta_fields"]["empresa"] = os.getenv("COMPANY_NAME")

        document_meta_fields["meta_fields"]["CNPJ"] = os.getenv("COMPANY_FORMATTED_CNPJ")

        document_meta_fields["meta_fields"]["origem"] = file_metadata[fmts.BucketCustomMetadata.SOURCE.value]

        document_meta_fields["meta_fields"]["tipo_documento"] = file_metadata[fmts.BucketCustomMetadata.DOCUMENT_TYPE.value]

        document_meta_fields["meta_fields"]["file_hash"] = file_metadata[fmts.BucketCustomMetadata.FILE_HASH.value]

        aggregation_type = file_metadata.get(fmts.BucketCustomMetadata.AGGREGATION_TYPE.value,None)

        if aggregation_type:

            document_meta_fields["meta_fields"]["tipo_agregacao_demonstrativo_financeiro"] = aggregation_type

        document_meta_fields["chunk_method"] = "naive"

        document_meta_fields["parser_config"] = self.NAIVE_PARSER_CONFIG

        doc = dataset.list_documents(name=treated_file_name)[0]

        doc.update(dict(document_meta_fields))

        self.logger.info("File metadata uploaded with success")

        self.logger.info("Requesting file parse (file reading + embedding)")

        dataset.async_parse_documents([doc.id])

        self.logger.info("File parse requested with success")
        


