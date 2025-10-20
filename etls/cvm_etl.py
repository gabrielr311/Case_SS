from bs4 import BeautifulSoup
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
from zipfile import ZipFile
import formats as fmts
from typing import List, Tuple
import pandas as pd
import network as network
import os
from xml.etree import ElementTree as ET
import base64
import json

class CVMETL:
    """
    Class responsible for the ETL processes for CVM's data
    It has 3 main components

    ITR and DFP ETL:
        Performs the ETL related to CVM's 'Cias Abertas: Documentos: Formulário de Informações Trimestrais (ITR)' and 'Formulário de Demonstrações Financeiras Padronizadas (DFP)'

    IPE ETL:
        Performs the ETL related to CVM'S 'Cias Abertas: Documentos: Periódicos e Eventuais (IPE)'

    FRE ETL:
        Performs the ETL related to CVM'S 'Cias Abertas: Documentos: Formulário de Referência (FRE)'

    """
    def __init__(self,config : fmts.IngestionOrchestratorConfig,cvm_company_code : str):
        self.config : fmts.IngestionOrchestratorConfig = config
        self.cvm_company_code = cvm_company_code
        self.data_source : fmts.DataSources = fmts.DataSources.CVM
        self.http_fixed_time_session : network.requests.Session = network.create_http_session(fixed_delay_retry=10,backoff_factor=0)
        self.http_exp_backoff_session : network.requests.Session = network.create_http_session(backoff_factor=1)

    ####CVM's etl specific formats

    FILE_NAME_PART_MAPPING = {
                            fmts.DocumentTypes.ITR: "cvm_itr",
                            fmts.DocumentTypes.IPE: "ipe_cia_aberta",
                            fmts.DocumentTypes.FRE: "fre_cia_aberta",
                            fmts.DocumentTypes.DFP : "dfp_cia_aberta"
                            }

    ITR_FILE_PART_TO_DOCUMENT_TYPE = {
                                    "BPA" :    fmts.DocumentTypes.BALANCO_PATRIMONIAL_ATIVO,
                                    "BPP" :    fmts.DocumentTypes.BALANCO_PATRIMONIAL_PASSIVO,
                                    "DFC_MI":  fmts.DocumentTypes.FLUXO_DE_CAIXA_INDIRETO,
                                    "DRE":     fmts.DocumentTypes.DRE
                                }
    
    IPE_EVENTS_MAPPING = {
                                "Fato Relevante":fmts.DocumentTypes.FATO_RELEVANTE, 
                                "Comunicado ao Mercado":fmts.DocumentTypes.COMUNICADOS_AO_MERCADO,
                                "Escrituras e aditamentos de debêntures":fmts.DocumentTypes.ESCRITURAS_E_ADITAMENTO_DE_DEBENTURES
                            }
    
    CNPJ_COLUMN_TO_FILTER = "CNPJ_CIA"
    ITR_END_OF_PERIOD_COLUMN_NAME = "DT_FIM_EXERC"
    ITR_EXERCISE_PERIOD_ORDER_COLUMN_NAME = "ORDEM_EXERC"
    FINANCIAL_STATEMENT_ADJUSTED_ACCOUNT_VALUE_COLUMN = "enr_adjusted_account_value"
    REF_DATE_COLUMN_NAME = "DT_REFER"
    DATE_FORMAT = "%Y-%m-%d"
    RAD_REF_DATE_FORMAT = "%d/%m/%Y"
    FRE_RECEIVAL_TIME_COLUMN_TO_FILTER = "DT_RECEB"
    FRE_DOWNLOAD_URL_COLUMN = "LINK_DOC"
    ITR_AMT_OF_YEARS_TO_FETCH = 3
    
    #####

    def CVM_check_for_updates(self,url: str) -> str:
        """
        Retrieves the last update date for the documents from CVM`s website though the  passed url.

        Args:

            url (str) : String to be requested
        
        Returns:
            str: The parsed string representing the last update date for CVM`S ITR company data
        """

        session = network.create_http_session(fixed_delay_retry=10,backoff_factor=0)

        response = session.get(url)

        assert response.status_code == 200, f"Failure in retrieving {response.status_code}"

        soup = BeautifulSoup(response.text, "html.parser")

        #additional_info_section = soup.find("section", class_="additional-info")

        # 1) Prefer locating the table row by the header text (robust when there are many similar spans)
        header = soup.find("th", string=lambda s: s and "Última Atualização" in s)
        span = None
        if header:
            row = header.find_parent("tr")
            if row:
                span = row.find("span", class_="automatic-local-datetime")

        # 2) Fallback: find the first span with that class anywhere
        if span is None:
            span = soup.find("span", class_="automatic-local-datetime")

        if not span:
            return None  # not found

        # 3) Prefer the ISO-like attribute if present
        dt_text = None
        if "data-datetime" in span.attrs:
            dt_text = span["data-datetime"].strip()
        else:
            # fallback to visible text like "outubro 6, 2025, 11:01 (UTC)"
            dt_text = span.get_text(" ", strip=True)

            # try to extract an ISO timestamp if embedded in the text
            m = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[+-]\d{4}|Z)?", dt_text)
            if m:
                dt_text = m.group(0)

        parsed_datetime = datetime.fromisoformat(dt_text).astimezone(ZoneInfo("America/Sao_Paulo"))

        return parsed_datetime.strftime(fmts.DateTimeFormats.FORMATTED_UP_TO_SECONDS.value)

    def CVM_download_and_upload_to_bucket(self,
                                          url : str,
                                          bronze_landing_obj_name : str,
                                          doc_type :fmts.DocumentTypes) ->List[str]:
        """
        Downloads the most recent .zip from the passed as url from CVM`s website though the
        passed url.

        Args:
            url (str)                     : String to be requested
            bronze_landing_obj_name (str) : Name of the file to be saved in the bronze/landing layer
            doc_type (fmts.DocumentTypes) : fmts.DocumentTypes to be used in the metadata
            

        Returns:
            List[str]: List containing the paths for the uploaded files in bronze raw.
        """

        bucket_name = self.config.minio_handler.bucket_name

        session = network.create_http_session(fixed_delay_retry=10,backoff_factor=0)

        self.config.logger.info(f"Starting to download the .zip file from '{url}'...")

        response = session.get(url)

        response.raise_for_status()
    
        self.config.minio_handler.save_file_to_bucket(
                                            bronze_landing_obj_name,
                                            BytesIO(response.content),
                                            fmts.create_ingest_ts(),
                                            self.config.trace_id,
                                            doc_type,
                                            fmts.ContentTypes.ZIP,
                                            self.data_source,
                                            ref_date = self.config.last_update_date
                                            )

        self.config.logger.info(f"Zip file downloaded and stored to {fmts.MedallionLayer.BRONZE_LANDING.path} with success")

        self.config.logger.info(f"Starting to unzip and upload contents to '{bucket_name}/bronze/raw'...")

        # Download ZIP into memory
        response,_ = self.config.minio_handler.get_file_bytes_and_metadata(bronze_landing_obj_name)
        zip_data = BytesIO(response.read())

        uploaded_keys = []

        with ZipFile(zip_data) as zip_ref:
            for member in zip_ref.namelist():
                # Flatten any internal directories
                file_name = member.split("/")[-1]
                if not file_name:  # skip directories
                    continue

                file_bytes = zip_ref.read(member)
                dest_key = f"{fmts.MedallionLayer.BRONZE_RAW.path}{file_name}"
                
                self.config.logger.info(f"Uploading file to '{fmts.MedallionLayer.BRONZE_RAW.path}{file_name}'...")

                self.config.minio_handler.save_file_to_bucket(
                                    dest_key,
                                    BytesIO(file_bytes),
                                    fmts.create_ingest_ts(),
                                    self.config.trace_id,
                                    doc_type,
                                    fmts.ContentTypes.PARQUET,
                                    self.data_source
                                    )

                
                uploaded_keys.append(dest_key)

                self.config.logger.info("File uploaded with success")

        return uploaded_keys

    def FS_filter_and_clean(self, 
                             files_path: List[str],
                             years_to_process : List[str]) -> List[str]:
        """
        Processes the financial statement files passed as arguments in order to remove data that isn't related to the desired company and also joins relevant data that is split in multiple files
        
        Args:
            files_path (List[str])                 : A list containing file paths to data in bronze/raw
            years_to_process : (List[str])         : The years that the files are from
        Returns:
            List[str]: List containing the paths for the uploaded parquet files in silver/cleaned.

        """

        FINANCIAL_STATEMENTS_GROUPING = {
                                        "_con_": fmts.CVMDocumentAggregationType.CONSOLIDADO,
                                        "_ind_": fmts.CVMDocumentAggregationType.INDIVIDUAL
                                    }

        EXERCISE_PERIOD_TO_CONSIDER = "ÚLTIMO"

        parquet_files_paths = list()

        for curr_year in years_to_process:
            
            for curr_agg_part,curr_agg in FINANCIAL_STATEMENTS_GROUPING.items():

                document_type_count = 1
                document_type_amt = len(self.ITR_FILE_PART_TO_DOCUMENT_TYPE)
                
                for curr_file_part,curr_document_type in self.ITR_FILE_PART_TO_DOCUMENT_TYPE.items():
                    
                    curr_document_schema = fmts.get_document_schema_from_doc_type(curr_document_type)

                    self.config.logger.info(f"Processing document type {document_type_count}/{document_type_amt}")

                    selected_files = [file_path for file_path in files_path if curr_file_part + curr_agg_part + curr_year in file_path]

                    joined_df : pd.DataFrame = pd.DataFrame()

                    for curr_file_path in selected_files:

                        self.config.logger.info(f"Processing file '{curr_file_path}'...")

                        file_from_bucket,_ = self.config.minio_handler.get_file_bytes_and_metadata(curr_file_path)

                        if not file_from_bucket:
                            self.config.minio_handler.logger.error(f"Failed to download file from bucket: '{curr_file_path}'")

                        file_bytes = BytesIO(file_from_bucket.read())

                        curr_file_df = pd.read_csv(
                            file_bytes,
                            dtype=curr_document_schema,
                            encoding=fmts.CVM_CSV_ENCODING,
                            sep=fmts.CVM_CSV_SEPARATOR, 
                        )

                        assert curr_file_df.columns.to_list() == list(curr_document_schema.keys()), self.config.minio_handler.logger.error(f"Downloaded IPE file schema doesnt match the one defined at DocumentSchemas.IPE")

                        curr_file_df = curr_file_df[curr_file_df[self.CNPJ_COLUMN_TO_FILTER] == self.config.formatted_cnpj]

                        assert not curr_file_df.empty, f"Failure while filtering the DF for file '{curr_file_path}'. Please confirm the provided company CNPJ '{self.config.formatted_cnpj}' in the environment file."

                        curr_file_df = curr_file_df[curr_file_df[self.ITR_EXERCISE_PERIOD_ORDER_COLUMN_NAME] == EXERCISE_PERIOD_TO_CONSIDER]

                        curr_file_df[self.REF_DATE_COLUMN_NAME] = pd.to_datetime(curr_file_df[self.REF_DATE_COLUMN_NAME], format=self.DATE_FORMAT).dt.date

                        curr_file_df["ORIGIN_FILE"] = "DFP" if "dfp_" in curr_file_path else "ITR"

                        joined_df = pd.concat([joined_df,curr_file_df],ignore_index=True)

                    save_file_path = self.config.get_bucket_save_file_path(fmts.MedallionLayer.SILVER_CLEANED,
                                                            self.data_source,
                                                            curr_document_type,
                                                            curr_agg,
                                                            ref_date=curr_year)

                    self.config.minio_handler.logger.info(f"Converting file of type '{curr_document_type.name}' and aggregation '{curr_agg.name}' to parquet and saving to '{save_file_path}'...")

                    parquet_bytes = self.config.convert_pandas_df_to_parquet_bytes(joined_df)

                    self.config.minio_handler.save_file_to_bucket(
                                    save_file_path,
                                    parquet_bytes,
                                    fmts.create_ingest_ts(),
                                    self.config.trace_id,
                                    curr_document_type,
                                    fmts.ContentTypes.PARQUET,
                                    self.data_source,
                                    ref_date=curr_year,
                                    agg_type=curr_agg
                                    )
                    
                    self.config.logger.info("Parquet file saved with success")

                    parquet_files_paths.append(save_file_path)

                    self.config.logger.info(f"Current document type '{curr_document_type.name}' processed with success")
                        
                    document_type_count += 1

        self.config.logger.info(f"All document types were processed and saved to '{fmts.MedallionLayer.SILVER_CLEANED.path}' with success")
                
        return parquet_files_paths

    def FS_enrich(self, 
                   silver_cleaned_files_path: List[str]) -> List[str]:
        """
        Joins the 'Balanço patrimonial' parquet that are split into passive and active. 
        Joins the financial statements across the years.
        Enrich data by creating columns starting with enr_
        
        Format values based on their 'ESCALA_MOEDA' field.
        
        Args:
            silver_cleaned_files_path (List[str])   : A list containing file paths to data in silver/cleaned
            years_to_process : (List[str])         : The years that the files are from

        Returns:
            List[str]: List containing the paths for the uploaded parquet files in silver/enriched.

        """

        #CVM doesn't provides an official mapping for the 'ESCALA_MOEDA' field so im manually creating it with data that I have seen
        ESCALA_MOEDA_MAPPING = {
                                "UNIDADE": 1,
                                "MIL":     1_000,
                                "MILHOES": 1_000_000, # This value wasnt documented in CVM's data schema, this is added here as a precaution
                                "MILHÕES": 1_000_000  # This value wasnt documented in CVM's data schema, this is added here as a precaution
                            } 
        agg_consolidado_dfs = dict()

        agg_individual_dfs = dict()

        self.config.logger.info("Enriching data from silver/cleaned and moving to silver/enriched...")

        for curr_file_path in silver_cleaned_files_path:

            parquet_file_bytes,file_metadata = self.config.minio_handler.get_file_bytes_and_metadata(curr_file_path)

            document_type,_ = fmts.get_document_type_and_schema_from_metadata(file_metadata)

            aggregation_type = file_metadata[fmts.BucketCustomMetadata.AGGREGATION_TYPE.value]

            curr_file_df = pd.read_parquet(parquet_file_bytes)

            curr_file_df[self.ITR_END_OF_PERIOD_COLUMN_NAME] = pd.to_datetime(curr_file_df[self.ITR_END_OF_PERIOD_COLUMN_NAME], format=self.DATE_FORMAT).dt.date

            #Enriching now, all enriched columns start with enr_

            curr_file_df["enr_adjusted_account_value"] = curr_file_df["VL_CONTA"] * curr_file_df["ESCALA_MOEDA"].map(ESCALA_MOEDA_MAPPING)

            curr_file_df["enr_last_updated_date"] = self.config.get_formatted_last_update_date()

            curr_file_df["enr_origin_document_type"] = document_type.value

            curr_file_df["enr_ingestion_trace_id"] = self.config.trace_id

            curr_file_df["enr_source"] = self.data_source.value

            curr_file_df["enr_aggregation_type"] = aggregation_type

            curr_file_df["enr_quarter"] = curr_file_df[self.ITR_END_OF_PERIOD_COLUMN_NAME].apply(fmts.get_quarter)

            curr_file_df["enr_year"] = curr_file_df[self.ITR_END_OF_PERIOD_COLUMN_NAME].apply(lambda x: x.year)

            if document_type == fmts.DocumentTypes.BALANCO_PATRIMONIAL_ATIVO:
                curr_file_df["enr_account_type"] = "ATIVO"

            if document_type == fmts.DocumentTypes.BALANCO_PATRIMONIAL_PASSIVO:
                curr_file_df["enr_account_type"] = "PASSIVO"

            dict_key = f"{curr_file_path.split('/')[-1]}"

            if aggregation_type == fmts.CVMDocumentAggregationType.INDIVIDUAL.value:

                agg_individual_dfs[dict_key] = curr_file_df

            elif aggregation_type == fmts.CVMDocumentAggregationType.CONSOLIDADO.value:

                agg_consolidado_dfs[dict_key] = curr_file_df

        self.config.logger.info("All data enriched with success")

        joined_consolidado_df = pd.concat(agg_consolidado_dfs, ignore_index=True)

        joined_individual_df = pd.concat(agg_individual_dfs, ignore_index=True)

        self.config.logger.info('Moving all enriched dataframes to silver/enriched...')

        document_type = fmts.DocumentTypes.DEMONSTRATIVOS_FINANCEIROS_FINAL

        silver_enriched_file_paths = []

        for curr_df,curr_agg in (
                        (joined_individual_df,fmts.CVMDocumentAggregationType.INDIVIDUAL),
                        (joined_consolidado_df,fmts.CVMDocumentAggregationType.CONSOLIDADO)
                    ):

            ref_date = fmts.create_ref_date(curr_df[self.ITR_END_OF_PERIOD_COLUMN_NAME].max())

            parquet_bytes = self.config.convert_pandas_df_to_parquet_bytes(curr_df)

            save_file_path = self.config.get_bucket_save_file_path(
                                                            layer=fmts.MedallionLayer.SILVER_ENRICHED,
                                                            source=self.data_source,
                                                            doc_type=document_type,
                                                            curr_agg=curr_agg,
                                                            ref_date=ref_date
                                                        )

            self.config.logger.info(f"Moving file of document type '{document_type}' to '{save_file_path}'...")

            self.config.minio_handler.save_file_to_bucket(
                                    save_file_path,
                                    parquet_bytes,
                                    fmts.create_ingest_ts(),
                                    self.config.trace_id,
                                    document_type,
                                    fmts.ContentTypes.PARQUET,
                                    self.data_source,
                                    ref_date = ref_date
                                    )

            silver_enriched_file_paths.append(save_file_path)

            self.config.logger.info("File moved with success")

        self.config.logger.info("All files have been enriched and moved to silver/enriched")

        return silver_enriched_file_paths

    def calculate_detailed_financials_quarterly(self,
                                       silver_enriched_files_path: List[str]):
        """
        Calculate financial metrics with detailed breakdown showing component accounts.
        Useful for debugging and understanding the calculations.
        
        Parameters:
        -----------
        df (pandas.DataFrame) : Combined dataframe with all 3 financial statements
        
        Returns:
        --------
            list[str] : Bucket file path of the uploaded files
        """

        dfs_map: dict[fmts.CVMDocumentAggregationType, pd.DataFrame] = {}

        for curr_df_file_path in silver_enriched_files_path:

            parquet_file_bytes,_ = self.config.minio_handler.get_file_bytes_and_metadata(curr_df_file_path)

            df = pd.read_parquet(parquet_file_bytes)
            
            def get_account_value(group_df, account_code, exact=False):
                """Helper function to get account value"""
                if exact:
                    mask = group_df['CD_CONTA'] == account_code
                else:
                    mask = group_df['CD_CONTA'].str.startswith(account_code)
                result = group_df.loc[mask, self.FINANCIAL_STATEMENT_ADJUSTED_ACCOUNT_VALUE_COLUMN].sum()
                return result if not pd.isna(result) else 0
            
            group_cols = ['CNPJ_CIA', 'DT_REFER']
            results = []
            
            for name, group in df.groupby(group_cols):
                cnpj, dt_refer = name
                
                # Revenue (Receita)
                revenue = get_account_value(group, '3.01', exact=True)
                
                # EBITDA components
                ebit = get_account_value(group, '3.05', exact=True)
                depreciation = get_account_value(group, '3.04.04', exact=True)
                ebitda = ebit + depreciation
                
                # Net Debt components (Dívida Líquida)
                debt_st = get_account_value(group, '2.01.04')
                debt_lt = get_account_value(group, '2.02.01')
                cash = get_account_value(group, '1.01.01', exact=True)
                total_debt = debt_st + debt_lt
                net_debt = total_debt - cash
                
                # Interest (Juros Pagos)
                interest_cf = get_account_value(group, '6.01.02.02')
                interest_is = get_account_value(group, '3.06.02', exact=True)
                interest_paid = interest_cf if interest_cf != 0 else interest_is
                
                # CAPEX (Capital Expenditure)
                capex = get_account_value(group, '6.02.01')
                
                # Working Capital Change (Variação de Capital de Giro)
                wc_change = get_account_value(group, '6.01.02')
                
                results.append({
                    'issuer_cnpj': cnpj,
                    'date': dt_refer,
                    'quarter' : fmts.get_quarter(dt_refer),
                    'year' : dt_refer.year,
                    'revenue': revenue,
                    'ebitda': ebitda,
                    'ebit': ebit,
                    'depreciation': depreciation,
                    'net_debt': net_debt,
                    'total_debt': total_debt,
                    'debt_short_term': debt_st,
                    'debt_long_term': debt_lt,
                    'cash': cash,
                    'interest_paid': abs(interest_paid),
                    'capex': abs(capex),
                    'wc_change': wc_change
                })
            
            curr_agg_quarterly_df = pd.DataFrame(results)

            agg_type = fmts.CVMDocumentAggregationType.INDIVIDUAL if "_INDIVIDUAL_" in curr_df_file_path else fmts.CVMDocumentAggregationType.CONSOLIDADO

            dfs_map[agg_type] = curr_agg_quarterly_df

        for curr_agg_type,curr_df in dfs_map.items():

            self.config.logger.info(f"Saving the final gold/serving '{fmts.GoldServingTableNames.FINANCIALS_QUARTERLY}' for aggregation '{curr_agg_type.value}'...")

            parquet_bytes = self.config.convert_pandas_df_to_parquet_bytes(curr_df)

            ref_date = fmts.create_ref_date(curr_df["date"].max())

            save_file_path = self.config.get_bucket_save_file_path(
                                                                fmts.MedallionLayer.GOLD_SERVING,
                                                                self.data_source,
                                                                doc_type=fmts.DocumentTypes.DEMONSTRATIVOS_FINANCEIROS_FINAL,
                                                                curr_agg=curr_agg_type,
                                                                ref_date=ref_date,
                                                                file_extension="parquet",
                                                                gold_serving_table_name=fmts.GoldServingTableNames.FINANCIALS_QUARTERLY
                                                            )

            self.config.minio_handler.save_file_to_bucket(
                                                            save_file_path,
                                                            parquet_bytes,
                                                            fmts.create_ingest_ts(),
                                                            self.config.trace_id,
                                                            fmts.DocumentTypes.DEMONSTRATIVOS_FINANCEIROS_FINAL,
                                                            fmts.ContentTypes.PARQUET,
                                                            self.data_source,
                                                            ref_date=ref_date,
                                                            agg_type=curr_agg_type
                                                            )
            
            self.config.redis_handler.save_to_cache(fmts.GoldServingTableNames.FINANCIALS_QUARTERLY,
                                curr_df.to_dict(orient="records"),
                                datetime.today(),
                                self.config.trace_id,
                                agg_type=curr_agg_type)

        excel_bytes = BytesIO()

        with pd.ExcelWriter(excel_bytes, engine="openpyxl") as writer:
            dfs_map[fmts.CVMDocumentAggregationType.CONSOLIDADO].to_excel(writer, sheet_name="DFS Consolidado", index=False)
            dfs_map[fmts.CVMDocumentAggregationType.INDIVIDUAL].to_excel(writer,  sheet_name="DFS Individual", index=False)

        excel_bytes.seek(0)

        ref_date = fmts.create_ref_date(dfs_map[fmts.CVMDocumentAggregationType.CONSOLIDADO]["date"].max())

        excel_file_path = self.config.get_bucket_save_file_path(
                                                            fmts.MedallionLayer.GOLD_EXPORT,
                                                            self.data_source,
                                                            doc_type=fmts.DocumentTypes.DEMONSTRATIVOS_FINANCEIROS_FINAL,
                                                            ref_date=ref_date,
                                                            file_extension="xlsx",
                                                            gold_serving_table_name=fmts.GoldServingTableNames.FINANCIALS_QUARTERLY
                                                        )

        self.config.minio_handler.save_file_to_bucket(
                                                        excel_file_path,
                                                        excel_bytes,
                                                        fmts.create_ingest_ts(),
                                                        self.config.trace_id,
                                                        fmts.DocumentTypes.DEMONSTRATIVOS_FINANCEIROS_FINAL,
                                                        fmts.ContentTypes.XLSX,
                                                        self.data_source
                                                        )
        

        self.config.logger.info(f"Excel file saved with success to '{excel_file_path}'...")

        self.config.logger.info("All gold layer files saved with success")

        return [excel_file_path]

    def CVM_startup(self,
             landing_url : str,
             download_zip_url : str,
             doc_type : fmts.DocumentTypes,
             file_year = str(datetime.today().year)) -> str:
        """
        Function responsible for checking if there`s new data and, in an affirmative case, downloads and stores it into the bronze/landing layer of the bucket.
        
        Args:
            landing_url (str)             : URL containing the landing page to download the zip. Used for checking if there`s new data
            download_zip_url (str)        : URL used to download the zip file if new documents were found
            doc_type (fmts.DocumentTypes) : Document type for the downloaded file. This is used in the bucket`s metadata
            file_year (str)               : Year to be used when downloading the files

        Returns:
            str: Bucket bronze/landing path of where the zip file was saved.

        """

        self.config.logger.info(f"Retrieving the last update date for CVM`s {doc_type.value}...")

        last_update_date = self.CVM_check_for_updates(landing_url)

        self.config.set_last_update_date(last_update_date)

        self.config.logger.info(f"Last update date fetched with success: {last_update_date}")

        self.config.logger.info(f"Checking if a file with this update date already exists in the path '{self.config.minio_handler.bucket_name}/bronze/landing'")

        file_name_in_bucket = f"{fmts.MedallionLayer.BRONZE_LANDING.path}{fmts.DataSources.CVM.value}/{self.FILE_NAME_PART_MAPPING[doc_type]}-{file_year}-{last_update_date}.zip"
        list_obj_ret = self.config.minio_handler.client.list_objects(
                                                bucket_name=self.config.minio_handler.bucket_name,
                                                prefix=file_name_in_bucket
                                            )

        found_files = [file for file in list_obj_ret]
        
        if found_files:
            self.config.logger.info(f"The file '{file_name_in_bucket}' is already present at the bucket so it wont be uploaded again")
            return 

        self.config.logger.info("There are new files available, starting to download them...")

        uploaded_csv_keys = self.CVM_download_and_upload_to_bucket(download_zip_url,file_name_in_bucket,doc_type)
 
        self.config.logger.info(f"Zip file for '{doc_type.value}' downloaded and uploaded to bronze/landing with success")
        
        return uploaded_csv_keys

    def IPE_filter_and_save_documents(self, file_path: str):
        """
        Processes the IPE file passed as arguments in order to filter out companies that arent the one being looked for
        and also filters only for the desired events noted at IPE_EVENTS_MAPPING. 
        This CSV file contains the IPE related events and a link to download the actual PDF documents related to these events
        
        Args:
            file_path (str): A file path to a .csv in bronze/raw containing the IPE .csv file

        """

        self.config.logger.info(f"Downloading file '{file_path}' from bucket...")

        file_from_bucket,_ = self.config.minio_handler.get_file_bytes_and_metadata(file_path)

        assert file_from_bucket, self.config.minio_handler.logger.error(f"Failed to download file from bucket: '{file_path}'")

        self.config.logger.info("CSV file downloaded with success, starting to process it...")

        file_bytes = BytesIO(file_from_bucket.read())

        curr_document_schema = fmts.get_document_schema_from_doc_type(fmts.DocumentTypes.IPE)

        ipe_events_df = pd.read_csv(
            file_bytes,
            dtype=curr_document_schema,
            encoding=fmts.CVM_CSV_ENCODING,
            sep=fmts.CVM_CSV_SEPARATOR, 
        )

        assert ipe_events_df.columns.to_list() == list(curr_document_schema.keys()), self.config.minio_handler.logger.error(f"Downloaded IPE file schema doesnt match the one defined at DocumentSchemas.IPE")

        ipe_events_df = ipe_events_df[ipe_events_df["CNPJ_Companhia"] == self.config.formatted_cnpj]

        assert not ipe_events_df.empty, f"Failure while filtering the DF for file '{file_path}'. Please confirm the provided company CNPJ '{self.config.formatted_cnpj}' in the environment file."

        ipe_events_df = ipe_events_df[ipe_events_df["Categoria"].isin(list(self.IPE_EVENTS_MAPPING.keys()))]

        categories_not_found = list(set(list(self.IPE_EVENTS_MAPPING.keys())) - set(ipe_events_df["Categoria"].unique().tolist()))

        if len(categories_not_found) == len(self.IPE_EVENTS_MAPPING):
            self.config.logger.error(f"No event categories from the filter list were found in the downloaded IPE .csv file for this company. Events filter list: {self.IPE_EVENTS_MAPPING}")
            raise Exception()

        if categories_not_found:
            self.config.logger.warning(f"The following categories werent found in the downloaded IPE .csv file: '{categories_not_found}'")
        else:
            self.config.logger.info("All categories were found in the IPE .csv file:")

        self.config.logger.info("File processed with success")

        self.config.logger.info(f"Downloading and saving the PDF files into gold/documents/pdfs'...")

        html_session = network.create_http_session()

        pdf_files_path = list()

        self.config.minio_handler.logger.info(f"There are {ipe_events_df.shape[0]} files to be downloaded")

        for idx,row in enumerate(ipe_events_df.itertuples(index=False)):

            self.config.minio_handler.logger.info(f"Downloading and uploading file {idx+1}/{ipe_events_df.shape[0]}")

            doc_type = self.IPE_EVENTS_MAPPING[row.Categoria]
            document_download_url = row.Link_Download
            document_version = row.Versao
            ref_date = "".join([char for char in row.Data_Referencia if char.isdigit()])
            
            response = html_session.get(document_download_url)

            response.raise_for_status()

            file_bytes = BytesIO(response.content)
            file_bytes.seek(0)

            save_file_path = self.config.get_bucket_save_file_path(
                                   fmts.MedallionLayer.GOLD_DOCUMENTS,
                                   self.data_source,
                                   doc_type,
                                   file_extension="pdf",
                                   document_version=document_version,
                                   ref_date=ref_date)

            self.config.logger.info(f"Uploading file to '{save_file_path}'")

            self.config.minio_handler.save_file_to_bucket(
                                                        save_file_path,
                                                        file_bytes,
                                                        fmts.create_ingest_ts(),
                                                        self.config.trace_id,
                                                        doc_type,
                                                        fmts.ContentTypes.PDF,
                                                        self.data_source
                                                        )

            self.config.logger.info("PDF file processed with success")

            pdf_files_path.append(save_file_path)

        return pdf_files_path
    
    def FRE_parse_and_strip_xml(self,xml_text_string: str) -> List[Tuple[str,BytesIO]]:
        """
        Extracts all PDF base64 strings inside <ImagemObjetoArquivoPdf> tags,
        returns a list of tuples (pdf_name, pdf_bytes),
        and returns a cleaned XML string with the base64 content removed.
        """

        root = ET.fromstring(xml_text_string)
        results: List[Tuple[str, BytesIO]] = []

        # Iterate over all elements in the XML tree
        for elem in root.iter():
            # We look for parent elements containing both NomeArquivoPdf and ImagemObjetoArquivoPdf
            nome_tag = elem.find(".//NomeArquivoPdf")
            img_tag = elem.find(".//ImagemObjetoArquivoPdf")

            if nome_tag is not None and img_tag is not None and img_tag.text:
                parent_tag_name = elem.tag
                pdf_data = base64.b64decode(img_tag.text.strip())
                buffer = BytesIO(pdf_data)
                buffer.seek(0)

                # We can use either parent_tag_name or nome_tag.text as file name
                results.append((parent_tag_name, buffer))

                img_tag.text=""

        cleaned_xml = ET.tostring(root, encoding="utf-8").decode("utf-8")
        cleaned_xml_bytes = BytesIO(cleaned_xml.encode(fmts.CVM_CSV_ENCODING))
        return results, cleaned_xml_bytes

    def FRE_filter_and_save_documents(self,file_path: str):
        """
        Processes the FRE file passed as arguments in order to filter out companies that arent the one being looked for.
        This CSV files contains a collection of FRE related data
        
        Args:
            file_path (str): File path to the .csv file 'fre_cia_aberta_YYYY.csv' in bronze/raw containing

        """

        self.config.logger.info(f"Downloading file '{file_path}' from bucket...")

        file_from_bucket,_ = self.config.minio_handler.get_file_bytes_and_metadata(file_path)

        assert file_from_bucket, self.config.minio_handler.logger.error(f"Failed to download file from bucket: '{file_path}'")

        self.config.logger.info("CSV file downloaded with success, starting to process it...")

        file_bytes = BytesIO(file_from_bucket.read())
        fre_events_df = pd.read_csv(
            file_bytes,
            encoding=fmts.CVM_CSV_ENCODING,
            sep=fmts.CVM_CSV_SEPARATOR, 
        )

        assert self.CNPJ_COLUMN_TO_FILTER in fre_events_df.columns.to_list(), self.config.logger.error(f"The file '{file_path}' did not contain the CNPJ filtering column '{self.CNPJ_COLUMN_TO_FILTER}'. Aborting ETL...")

        assert self.FRE_RECEIVAL_TIME_COLUMN_TO_FILTER in fre_events_df.columns.to_list(), self.config.logger.error(f"The file '{file_path}' did not contain the date filtering column '{self.FRE_RECEIVAL_TIME_COLUMN_TO_FILTER}'. Aborting ETL...")

        fre_events_df = fre_events_df[fre_events_df[self.CNPJ_COLUMN_TO_FILTER] == self.config.formatted_cnpj]

        fre_events_df[self.FRE_RECEIVAL_TIME_COLUMN_TO_FILTER] = pd.to_datetime(fre_events_df[self.FRE_RECEIVAL_TIME_COLUMN_TO_FILTER], format=self.DATE_FORMAT).dt.date

        fre_events_df[self.REF_DATE_COLUMN_NAME] = pd.to_datetime(fre_events_df[self.REF_DATE_COLUMN_NAME], format=self.DATE_FORMAT).dt.date

        most_recent_fre_document_row = fre_events_df.loc[fre_events_df[self.FRE_RECEIVAL_TIME_COLUMN_TO_FILTER].idxmax()]

        ref_date = most_recent_fre_document_row[self.REF_DATE_COLUMN_NAME]

        download_link = most_recent_fre_document_row[self.FRE_DOWNLOAD_URL_COLUMN]

        self.config.logger.info(f"Downloading the zip file containing XML files for the most recent FRE for reference date '{ref_date.strftime("%d-%m-%Y")}' with url '{download_link}'...")

        response = self.http_exp_backoff_session.get(download_link)

        response.raise_for_status()

        self.config.logger.info("Zip file downloaded with success. Starting to process the internal XML file...")

        zip_data = BytesIO(response.content)
        zip_data.seek(0)

        fre_file_pattern = f"{fmts.DocumentTypes.FRE.value}{ref_date.strftime("%d-%m-%Y")}"

        with ZipFile(zip_data) as zip_ref:

            xml_fre_file_list = [file_name for file_name in zip_ref.namelist() if fre_file_pattern in file_name]
            
            assert xml_fre_file_list, self.config.logger.error(f"FRE file from url '{download_link}' did not contain the expected pattern '{fre_file_pattern}'")
            
            xml_fre_file = xml_fre_file_list[0]

            self.config.logger.info(f"Expected XML file found with success. File name: '{xml_fre_file}'. Starting to read its contents...")
            
            with zip_ref.open(xml_fre_file) as f:
                text = f.read().decode(fmts.CVM_CSV_ENCODING)
                pdf_file_names_and_bytes,cleaned_xml_bytes = self.FRE_parse_and_strip_xml(text)

        self.config.logger.info("All PDF files bytes and cleaned XML were extracted from the original XML file. Starting to store them into the gold/exports layer...")

        self.config.logger.info("Uploading PDF files...")

        processed_file_paths = list()

        for pdf_file_name,pdf_bytes in pdf_file_names_and_bytes:

            pdf_save_file_path = self.config.get_bucket_save_file_path(
                                                  layer=fmts.MedallionLayer.GOLD_DOCUMENTS,
                                                  source=fmts.DataSources.CVM,
                                                  doc_type=fmts.DocumentTypes.FRE,
                                                  file_extension="pdf",
                                                  ref_date=fmts.create_ref_date(ref_date),
                                                  additional_name_part=pdf_file_name)
            
            self.config.minio_handler.save_file_to_bucket(
                    pdf_save_file_path,
                    pdf_bytes,
                    fmts.create_ingest_ts(),
                    self.config.trace_id,
                    fmts.DocumentTypes.FRE,
                    fmts.ContentTypes.PDF,
                    self.data_source,
                    ref_date=fmts.create_ref_date(ref_date)
                                    )
            
            processed_file_paths.append(pdf_save_file_path)
            
        self.config.logger.info("All PDF files saved with success. Saving the cleaned XML file...")

        xml_save_file_path = self.config.get_bucket_save_file_path(
                                                layer=fmts.MedallionLayer.GOLD_DOCUMENTS,
                                                source=fmts.DataSources.CVM,
                                                doc_type=fmts.DocumentTypes.FRE,
                                                file_extension="xml",
                                                ref_date=fmts.create_ref_date(ref_date),
                                                additional_name_part="CLEANED_XML")
        
        self.config.minio_handler.save_file_to_bucket(
                xml_save_file_path,
                cleaned_xml_bytes,
                fmts.create_ingest_ts(),
                self.config.trace_id,
                fmts.DocumentTypes.FRE,
                fmts.ContentTypes.XML,
                self.data_source,
                ref_date=fmts.create_ref_date(ref_date)
                                )
        
        processed_file_paths.append(xml_save_file_path)

        return processed_file_paths

    def FS_full_etl(self):
        """
        Function responsible for the entire ETL process for CVM's 'Cias Abertas: Documentos: Formulário de Informações Trimestrais (ITR)' and  'Formulário de Demonstrações Financeiras Padronizadas (DFP)' documents.
        This function has the following steps
            1 - Check if there's new data from 'CVM_IPE_LANDING_URL' based on the last update data available.
                In the event of no new data, the ETL finishes at this step without requesting the actual files from CVM
                This is done in order not to overly request their system unnecessarily

            2 - If there's new data, download the .zip file from 'CVM_IPE_DOWNLOAD_RAW_URL'. 
                This URL uses fmts.PLACEHOLDER_VALUE to allow for replacing the year part of the required file.
                The .zip file is then downloaded and stored at bronze/landing

            3 - The downloaded .zip file gets extracted and its content are a collection of .csv files containing the financial statements.
                The financial statements are also separated by aggregation levels, as seen in fmts.CVMDocumentAggregationType.
                This aggregation allows the analysis of the entire holding group or only from a specific subsidiary.
                After unzipping, the .csv files are filtered out by the desired company's CNPJ and then they are stored at silver/cleaned

            4 - The files at silver/cleaned are then downloaded in order to be enriched with additional relevant data and, after this, are saved to silver/enriched.

            5 - The files at silver/enriched are finally normalized and store at gold/serving for .parquet files and at gold/export for human usable Excel files

        """

        try:

            years_to_fetch_data = [str(datetime.today().year - i) for i in range(0, self.ITR_AMT_OF_YEARS_TO_FETCH)]

            all_years_uploaded_csv_keys = []

            self.config.logger.info(f"Fetching {fmts.DocumentTypes.ITR.value} files for the following years: {years_to_fetch_data}")

            urls_tuple = (
                        (os.getenv("CVM_ITR_LANDING_URL"),os.getenv("CVM_ITR_DOWNLOAD_RAW_URL")),
                        (os.getenv("CVM_DFP_LANDING_URL"),os.getenv("CVM_DFP_DOWNLOAD_RAW_URL")),
                        )

            for curr_year in years_to_fetch_data:

                for curr_landing_url,curr_raw_download_url in urls_tuple:

                    doc_type = fmts.DocumentTypes.ITR if "ITR" in curr_raw_download_url else fmts.DocumentTypes.DFP

                    if (curr_year == str(datetime.today().year) and doc_type == fmts.DocumentTypes.DFP):
                        self.config.logger.warning(f"Skipping DFP download for '{curr_year}' because its not available yet...")
                        continue

                    self.config.logger.info(f"Fetching files for '{curr_year}'...")

                    treated_download_url = curr_raw_download_url.replace(fmts.PLACEHOLDER_VALUE,curr_year)

                    uploaded_csv_keys = self.CVM_startup(
                                                    curr_landing_url,
                                                    treated_download_url,
                                                    doc_type,
                                                    file_year=curr_year)
                    
                    if not uploaded_csv_keys:
                
                        self.config.logger.info(f"No new data for file type '{fmts.DocumentTypes.ITR.value}' and '{fmts.DocumentTypes.DFP.value}'. Aborting this ETL...")
                        return

                    all_years_uploaded_csv_keys.extend(uploaded_csv_keys)

                    self.config.logger.info(f"'{curr_year}' uploaded to the bucket with success")

            if not uploaded_csv_keys:
                
                self.config.logger.info(f"No new data for file type '{fmts.DocumentTypes.ITR.value}'. Aborting this ETL...")
                return

            #all_years_uploaded_csv_keys = ['bronze/raw/itr_cia_aberta_2025.csv', 'bronze/raw/itr_cia_aberta_BPA_con_2025.csv', 'bronze/raw/itr_cia_aberta_BPA_ind_2025.csv', 'bronze/raw/itr_cia_aberta_BPP_con_2025.csv', 'bronze/raw/itr_cia_aberta_BPP_ind_2025.csv', 'bronze/raw/itr_cia_aberta_composicao_capital_2025.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_con_2025.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_ind_2025.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_con_2025.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_ind_2025.csv', 'bronze/raw/itr_cia_aberta_DMPL_con_2025.csv', 'bronze/raw/itr_cia_aberta_DMPL_ind_2025.csv', 'bronze/raw/itr_cia_aberta_DRA_con_2025.csv', 'bronze/raw/itr_cia_aberta_DRA_ind_2025.csv', 'bronze/raw/itr_cia_aberta_DRE_con_2025.csv', 'bronze/raw/itr_cia_aberta_DRE_ind_2025.csv', 'bronze/raw/itr_cia_aberta_DVA_con_2025.csv', 'bronze/raw/itr_cia_aberta_DVA_ind_2025.csv', 'bronze/raw/itr_cia_aberta_parecer_2025.csv', 'bronze/raw/itr_cia_aberta_2024.csv', 'bronze/raw/itr_cia_aberta_BPA_con_2024.csv', 'bronze/raw/itr_cia_aberta_BPA_ind_2024.csv', 'bronze/raw/itr_cia_aberta_BPP_con_2024.csv', 'bronze/raw/itr_cia_aberta_BPP_ind_2024.csv', 'bronze/raw/itr_cia_aberta_composicao_capital_2024.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_con_2024.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_ind_2024.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_con_2024.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_ind_2024.csv', 'bronze/raw/itr_cia_aberta_DMPL_con_2024.csv', 'bronze/raw/itr_cia_aberta_DMPL_ind_2024.csv', 'bronze/raw/itr_cia_aberta_DRA_con_2024.csv', 'bronze/raw/itr_cia_aberta_DRA_ind_2024.csv', 'bronze/raw/itr_cia_aberta_DRE_con_2024.csv', 'bronze/raw/itr_cia_aberta_DRE_ind_2024.csv', 'bronze/raw/itr_cia_aberta_DVA_con_2024.csv', 'bronze/raw/itr_cia_aberta_DVA_ind_2024.csv', 'bronze/raw/itr_cia_aberta_parecer_2024.csv', 'bronze/raw/dfp_cia_aberta_2024.csv', 'bronze/raw/dfp_cia_aberta_BPA_con_2024.csv', 'bronze/raw/dfp_cia_aberta_BPA_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_BPP_con_2024.csv', 'bronze/raw/dfp_cia_aberta_BPP_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_composicao_capital_2024.csv', 'bronze/raw/dfp_cia_aberta_DFC_MD_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DFC_MD_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_DFC_MI_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DFC_MI_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_DMPL_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DMPL_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_DRA_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DRA_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_DRE_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DRE_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_DVA_con_2024.csv', 'bronze/raw/dfp_cia_aberta_DVA_ind_2024.csv', 'bronze/raw/dfp_cia_aberta_parecer_2024.csv', 'bronze/raw/itr_cia_aberta_2023.csv', 'bronze/raw/itr_cia_aberta_BPA_con_2023.csv', 'bronze/raw/itr_cia_aberta_BPA_ind_2023.csv', 'bronze/raw/itr_cia_aberta_BPP_con_2023.csv', 'bronze/raw/itr_cia_aberta_BPP_ind_2023.csv', 'bronze/raw/itr_cia_aberta_composicao_capital_2023.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_con_2023.csv', 'bronze/raw/itr_cia_aberta_DFC_MD_ind_2023.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_con_2023.csv', 'bronze/raw/itr_cia_aberta_DFC_MI_ind_2023.csv', 'bronze/raw/itr_cia_aberta_DMPL_con_2023.csv', 'bronze/raw/itr_cia_aberta_DMPL_ind_2023.csv', 'bronze/raw/itr_cia_aberta_DRA_con_2023.csv', 'bronze/raw/itr_cia_aberta_DRA_ind_2023.csv', 'bronze/raw/itr_cia_aberta_DRE_con_2023.csv', 'bronze/raw/itr_cia_aberta_DRE_ind_2023.csv', 'bronze/raw/itr_cia_aberta_DVA_con_2023.csv', 'bronze/raw/itr_cia_aberta_DVA_ind_2023.csv', 'bronze/raw/itr_cia_aberta_parecer_2023.csv', 'bronze/raw/dfp_cia_aberta_2023.csv', 'bronze/raw/dfp_cia_aberta_BPA_con_2023.csv', 'bronze/raw/dfp_cia_aberta_BPA_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_BPP_con_2023.csv', 'bronze/raw/dfp_cia_aberta_BPP_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_composicao_capital_2023.csv', 'bronze/raw/dfp_cia_aberta_DFC_MD_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DFC_MD_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_DFC_MI_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DFC_MI_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_DMPL_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DMPL_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_DRA_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DRA_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_DRE_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DRE_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_DVA_con_2023.csv', 'bronze/raw/dfp_cia_aberta_DVA_ind_2023.csv', 'bronze/raw/dfp_cia_aberta_parecer_2023.csv']
            self.config.logger.info("Starting bronze layer processing...")

            self.config.logger.info("Bronze layer processing finished with success")

            self.config.logger.info("Starting silver layer processing...")

            silver_cleaned_files_path = self.FS_filter_and_clean(all_years_uploaded_csv_keys,years_to_fetch_data)

            silver_enriched_file_paths = self.FS_enrich(silver_cleaned_files_path)

            self.config.logger.info("Silver layer processing finished with success")

            self.config.logger.info("Starting gold layer processing...")

            uploaded_file_paths = self.calculate_detailed_financials_quarterly(silver_enriched_file_paths)

            self.config.logger.info("Gold layer processing finished with success")

            self.config.logger.info("All financial data from CVM's ITR and DFP have been processed with success and are now available at the gold layer for consumption")

            return uploaded_file_paths
        
        except Exception as e:

            raise Exception(f"CVMETL:FS_full_etl: {e}")


    def IPE_full_etl(self):
        """
        Function responsible for the entire ETL process for CVM's 'Cias Abertas: Documentos: Periódicos e Eventuais (IPE)' documents.
        This function has the following steps
            1 - Check if there's new data from 'CVM_IPE_LANDING_URL' based on the last update data available.
                In the event of no new data, the ETL finishes at this step without requesting the actual files from CVM
                This is done in order not to overly request their system unnecessarily

            2 - If there's new data, download the .zip file from 'CVM_IPE_DOWNLOAD_RAW_URL'. 
                This URL uses fmts.PLACEHOLDER_VALUE to allow for replacing the year part of the required file.
                The .zip file is then downloaded and stored at bronze/landing

            3 - The downloaded .zip file gets extracted and its content is a single CSV file containing all of the IPE events.
                The process then filters out the CNPJ that aren't of the desired company and also filters the events by column
                'Categoria' based on the allow list 'IPE_EVENTS_MAPPING'. 
                Each row contains an IPE event and also a download link to the pdf file containing the actual event

            4 - The pdf files are then download and moved to gold/documents/pdfs
        """
        self.config.logger.info("Starting CVM's IPE full ETL...")

        try:

            uploaded_csv_keys = self.CVM_startup(
                                                os.getenv("CVM_IPE_LANDING_URL"),
                                                os.getenv("CVM_IPE_DOWNLOAD_RAW_URL").replace(fmts.PLACEHOLDER_VALUE,str(datetime.today().year)),
                                                fmts.DocumentTypes.IPE
                                            )

            if not uploaded_csv_keys:
                
                self.config.logger.info(f"No new data for file type '{fmts.DocumentTypes.IPE.value}'. Aborting this ETL...")
                return

            #For the IPE there`s only a single .csv file inside the downloaded .zip file
            file_key = uploaded_csv_keys[0]

            treated_keys = self.IPE_filter_and_save_documents(file_key)

            self.config.logger.info("All IPE documents")

            return treated_keys
        
        except Exception as e:

            raise Exception(f"CVMETL:IPE_full_etl: {e}")

            
    def FRE_full_etl(self):
        """
        Function responsible for the entire ETL process for CVM's 'Cias Abertas: Documentos: Periódicos e Eventuais (IPE)' documents.
        This function has the following steps
            1 - Check if there's new data from 'CVM_IPE_LANDING_URL' based on the last update data available.
                In the event of no new data, the ETL finishes at this step without requesting the actual files from CVM
                This is done in order not to overly request their system unnecessarily

            2 - If there's new data, download the .zip file from 'CVM_IPE_DOWNLOAD_RAW_URL'. 
                This URL uses fmts.PLACEHOLDER_VALUE to allow for replacing the year part of the required file.
                The .zip file is then downloaded and stored at bronze/landing

            3 - The downloaded .zip file gets extracted and its content is a single CSV file containing all of the IPE events.
                The process then filters out the CNPJ that aren't of the desired company and also filters the events by column
                'Categoria' based on the allow list 'IPE_EVENTS_MAPPING'. 
                Each row contains an IPE event and also a download link to the pdf file containing the actual event

            4 - The pdf files are then download and moved to gold/documents/pdfs
        """
        self.config.logger.info("Starting CVM's IPE full ETL...")

        try:

            uploaded_csv_keys = self.CVM_startup(
                                                os.getenv("CVM_FRE_LANDING_URL"),
                                                os.getenv("CVM_FRE_DOWNLOAD_RAW_URL").replace(fmts.PLACEHOLDER_VALUE,str(datetime.today().year)),
                                                fmts.DocumentTypes.FRE
                                            )

            if not uploaded_csv_keys:
                
                self.config.logger.info(f"No new data for file type '{fmts.DocumentTypes.FRE.value}'. Aborting this ETL...")
                return

            file_part_to_find  = f"{self.FILE_NAME_PART_MAPPING[fmts.DocumentTypes.FRE]}_{datetime.now().year}.csv"

            fre_file_path = [file for file in uploaded_csv_keys if file_part_to_find in file]

            assert fre_file_path, self.config.logger.info(f"Expected .csv FRE file with name part '{file_part_to_find}' wasn't found inside the .zip file")

            fre_file_path = fre_file_path[0]

            treated_keys = self.FRE_filter_and_save_documents(fre_file_path)

            self.config.logger.info("All FRE documents were ingested with success")

            return treated_keys

        except Exception as e:

            raise Exception(f"CVMETL:FRE_full_etl: {e}")


    def parse_RAD_search_results_table(self,table_text: str) -> pd.DataFrame:
        """
        Function responsible for parsing the table text thats returned from RAD after document searches

        Args:

            table_text (str) : String containing the table text from RAD's search result table

        Returns:

            pd.Dataframe : Dataframe containing the structured data from the table text
        """

        # Split rows
        rows = [r for r in table_text.split('*') if r.strip()]
        
        parsed_rows = []
        for row in rows:
            cols = row.split('$&')
            # pad columns if missing
            cols += [""] * (12 - len(cols))
            
            company_code = cols[0].strip()
            company_name = cols[1].strip()
            category = cols[2].strip()
            doc_type = cols[3].strip()
            title = re.sub(r'<.*?>', '', cols[4]).strip()
            ref_date = re.search(r"(\d{2}/\d{2}/\d{4})", cols[5])
            ref_date_str = ref_date.group(1) if ref_date else None
            delivery_date = re.search(r"(\d{2}/\d{2}/\d{4})", cols[6])
            delivery_date_str = delivery_date.group(1) if delivery_date else None
            status = cols[7].strip()
            
            # extract IDs from buttons HTML (col 10)
            html_buttons = cols[10]
            protocol_match = re.search(r"NumeroProtocoloEntrega=(\d+)", html_buttons)
            protocol_id = protocol_match.group(1) if protocol_match else None
            
            download_match = re.search(r"OpenDownloadDocumentos\('(\d+)'", html_buttons)
            download_id = download_match.group(1) if download_match else None
            
            pub_info_match = re.search(r"mostraLocaisPublicacao\('[^']+', '([^']+)'\)", html_buttons)
            publication_info = pub_info_match.group(1) if pub_info_match else None
            
            parsed_rows.append({
                "company_code": company_code,
                "company_name": company_name,
                "category": category,
                "document_type": doc_type,
                "title": title,
                "reference_date": ref_date_str,
                "delivery_date": delivery_date_str,
                "status": status,
                "protocol_id": protocol_id,
                "download_id": download_id,
                "publication_info": publication_info
            })
        
        return pd.DataFrame(parsed_rows)

    def RAD_download_document(self,
                              doc_code : fmts.CVMRADDocumentCodes,
                              years_to_search: int) -> List[str]:
        
        """
        Download documents from CVM's RAD webpage and uploads to the bucket

        Return:
            List[str]: List with the files uploaded to the bucket
        """

        # Searches for the last 3 years
        start_date = datetime(datetime.today().year - years_to_search,1,1).strftime(self.RAD_REF_DATE_FORMAT)

        end_date = datetime.today().strftime(self.RAD_REF_DATE_FORMAT)

        payload = json.dumps(
                             { 
                                'dataDe': start_date, 'dataAte': end_date , 'empresa': f',{self.cvm_company_code}', 
                                'setorAtividade': '-1', 'categoriaEmissor': '-1', 'situacaoEmissor': '-1', 
                                'tipoParticipante': '-1', 'dataReferencia': '', 'categoria': doc_code.value, 
                                'periodo': '2', 'horaIni': '', 'horaFim': '', 'palavraChave':'','ultimaDtRef':'false', 
                                'tipoEmpresa':'0', 'token': '', 'versaoCaptcha': ''
                              }
                            )
        
        headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json; charset=UTF-8',
        'Origin': 'https://www.rad.cvm.gov.br',
        'Referer': 'https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        }

        url = os.getenv("CVM_RAD_SEARCH_PAGE")

        session = self.http_exp_backoff_session

        response = session.post(url, headers=headers, data=payload)

        response.raise_for_status()

        data_str = json.loads(response.content)["d"]["dados"]

        search_results_df = self.parse_RAD_search_results_table(data_str)

        results_amt = search_results_df.shape[0]

        if not results_amt:

            self.config.logger.info(f"No results were returned from RAD while searching for document '{doc_code}'")

        self.config.logger.info(f"There are '{results_amt}' returned documents from the search. Starting to process them...")

        processed_files_path : list[str] = list()

        for idx,row in enumerate(search_results_df.itertuples(index=False)):

            self.config.logger.info(f"Processing file {idx+1}/{results_amt}")

            protocol_download_id = row.protocol_id

            ref_date = row.reference_date

            category = row.category
            
            assert protocol_download_id, self.config.logger.error(f"No protocol download id was found for passed document '{doc_code.name}' with code '{doc_code.value}'")

            assert ref_date, self.config.logger.error(f"No reference date was found passed document '{doc_code.name}' with code '{doc_code.value}'")

            # Convert date string to datetime
            ref_date = datetime.strptime(ref_date, self.RAD_REF_DATE_FORMAT).strftime(fmts.DateTimeFormats.FORMATTED_DATE_ONLY.value) if ref_date else None

            self.config.logger.info(f"Protocol download ID found '{protocol_download_id}'. Starting to download file of category '{category}'...")

            url = os.getenv("CVM_RAD_DOWNLOAD_FILE_URL")

            payload = json.dumps(
                                    { "codigoInstituicao": '1', 
                                    "numeroProtocolo": protocol_download_id, 
                                    "token": '', 
                                    "versaoCaptcha": ''
                                    }
                                )

            headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': 'https://www.rad.cvm.gov.br',
            'Referer': f'https://www.rad.cvm.gov.br/ENET/frmExibirArquivoIPEExterno.aspx?NumeroProtocoloEntrega={protocol_download_id}',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
            }

            response = session.post(url, headers=headers, data=payload)

            response.raise_for_status()

            pdf_base64 = json.loads(response.content)["d"].strip()

            pdf_data = base64.b64decode(pdf_base64)

            buffer = BytesIO(pdf_data)

            treated_title = row.title.replace(" ","_")

            save_file_path = self.config.get_bucket_save_file_path(fmts.MedallionLayer.GOLD_DOCUMENTS,
                                                    self.data_source,
                                                    fmts.DocumentTypes[doc_code.name],
                                                    ref_date=ref_date,
                                                    file_extension = "pdf",
                                                    additional_name_part=f"{treated_title}")

            self.config.logger.info(f"Saving file '{doc_code.name}' to '{save_file_path}'...")

            self.config.minio_handler.save_file_to_bucket(
                            save_file_path,
                            buffer,
                            fmts.create_ingest_ts(),
                            self.config.trace_id,
                            fmts.DocumentTypes[doc_code.name],
                            fmts.ContentTypes.PDF,
                            self.data_source,
                            ref_date=ref_date
                            )
            
            processed_files_path.append(save_file_path)

            self.config.logger.info(f"File '{doc_code.name}' from RAD processed with success.")

        self.config.logger.info(f"All files from search retrieved with success '{doc_code.name}' from RAD processed with success.")

        return processed_files_path

    def RAD_document_full_etl(self):
        """
            Function responsible for downloading all of the RAD documents from CVM described in fmts.CVM_RAD_DOCUMENTS_TO_DOWNLOAD
        """

        doc_types = [
                      (fmts.CVMRADDocumentCodes.DADOS_ECONOMICO_E_FINANCEIROS,1),
                      (fmts.CVMRADDocumentCodes.ESCRITURAS_E_ADITAMENTO_DE_DEBENTURES,5)
                    ]

        processed_file_paths : List[str] = list()

        for curr_doc_type,years_to_search in doc_types:

            try:

                processed_file = self.RAD_download_document(curr_doc_type,years_to_search)

                processed_file_paths.extend(processed_file)

            except Exception as e:

                raise Exception(f"CVMETL:RAD_download_document: {e}")
    
        return processed_file_paths


