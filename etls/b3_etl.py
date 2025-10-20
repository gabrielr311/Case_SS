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

class B3ETL:
    """
    Class responsible for the ETL processes for B3's data
    It has 3 main components

    Macroeconomic data ETL:
        Performs the ETL related to macroeconomic data (rates, inflation,FX) from B3

    IPE ETL:
        Performs the ETL related to CVM'S 'Cias Abertas: Documentos: Periódicos e Eventuais (IPE)'

    FRE ETL:
        Performs the ETL related to CVM'S 'Cias Abertas: Documentos: Formulário de Referência (FRE)'

    """
    def __init__(self,config : fmts.IngestionOrchestratorConfig):
        self.config : fmts.IngestionOrchestratorConfig = config
        self.data_source : fmts.DataSources = fmts.DataSources.B3
        self.http_fixed_time_session : network.requests.Session = network.create_http_session(fixed_delay_retry=10,backoff_factor=0)
        self.http_exp_backoff_session : network.requests.Session = network.create_http_session(backoff_factor=1)

    MACRO_DATA_SECURITY_ID      = "securityIdentificationCode"
    MACRO_DATA_DESCRIPTION      = "description"
    MACRO_DATA_TYPE             = "groupDescription"
    MACRO_DATA_VALUE            = "value"
    MACRO_DATA_RATE             = "rate"
    MACRO_DATA_LAST_UPDATE_DATE = "lastUpdate"

    MACRO_DATA_TYPE_MAP = {
                           "TAXAS DE CÂMBIO"              : "FX",
                           "TAXAS DE JUROS INTERNACIONAL" : "INTERNATIONAL_RATES",
                           "TAXAS DE JUROS NACIONAL"      : "DOMESTIC_RATES"
                           }
    

    def macro_data_full_etl(self)  -> List[str]:
        """
        Function responsible for the entire ETL process for B3's macroeconomid data (rates, inflation,FX)

        Return:
            List[str]: List of all file paths uploaded to minio bucket.
        """
        try:
            self.config.logger.info("Starting to retrieve macroeconomic data from B3...")

            url = os.getenv("B3_MACRO_DATA_URL")
            payload={}
            headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
            }
            session = self.http_exp_backoff_session

            response = session.request("GET", url, headers=headers, data=payload)

            response.raise_for_status()

            all_macro_data = json.loads(response.content)

            df_data_list = list()

            for curr_macro_data in all_macro_data:

                security_id = curr_macro_data[self.MACRO_DATA_SECURITY_ID]

                description = curr_macro_data[self.MACRO_DATA_DESCRIPTION]

                data_type = self.MACRO_DATA_TYPE_MAP[curr_macro_data[self.MACRO_DATA_TYPE]]

                #This is done because both fields are always provided but only one of them is actually filled
                value = max(curr_macro_data[self.MACRO_DATA_VALUE],curr_macro_data[self.MACRO_DATA_RATE])

                last_updated_data = curr_macro_data[self.MACRO_DATA_LAST_UPDATE_DATE]

                df_data_list.append([security_id,description,data_type,value,last_updated_data])

            macro_data_df : pd.DataFrame = pd.DataFrame(data=df_data_list,columns=fmts.DocumentSchemas.B3_DADOS_MACRO.get_column_names)
            
            macro_data_df = fmts.convert_brazilian_numbers_to_float(macro_data_df,["value"])

            #Guaranteeing that data adheres to schema
            macro_data_df = macro_data_df.astype(fmts.DocumentSchemas.B3_DADOS_MACRO.value)
            macro_data_df["ref_date"] = macro_data_df["ref_date"].dt.date

            self.config.logger.info("All macro data processed with success, saving to gold layer...")

            ref_date = fmts.create_ref_date(datetime.today())

            saved_file_map = self.config.save_df_to_gold_export_and_serving(
                                            macro_data_df,
                                            self.data_source,
                                            fmts.DocumentTypes.B3_DADOS_MACRO,
                                            ref_date,
                                            fmts.GoldServingTableNames.MACRO_DATA
                                            )
            
            
            self.config.redis_handler.save_to_cache(fmts.GoldServingTableNames.MACRO_DATA,
                                                    macro_data_df.to_dict(orient="records"),
                                                    datetime.today(),
                                                    self.config.trace_id)

            self.config.logger.info("All macro data files saved to gold layer, ending this ETL with success...")

            gold_export_file_path = [saved_file_map[fmts.MedallionLayer.GOLD_EXPORT]]

            return gold_export_file_path

        except Exception as e:

            raise Exception(f"B3ETL:macro_data_full_etl: {e}")


        