from bs4 import BeautifulSoup
import re
from datetime import datetime
from io import BytesIO
import formats as fmts
from typing import List, Tuple
import pandas as pd
import network as network
import os

class SNDETL:
    """
    Class responsible for the ETL processes for SNDs's data through debentures.com.br portal
    It has 3 main components

    Financial events schedule ETL:
        Performs the ETL related to the debenture's financial events schedule, with data such as: event date, payment date, event, yield, indexer etc.

    Secondary markets traded prices ETL:
        Performs the ETL related to the debenture's secondary markets traded prices

    Debenture terms ETL:
        Performs the ETL related to the debenture's terms
    """
    def __init__(self,config : fmts.IngestionOrchestratorConfig):
        self.config : fmts.IngestionOrchestratorConfig = config
        self.data_source : fmts.DataSources = fmts.DataSources.SND
        self.http_fixed_time_session : network.requests.Session = network.create_http_session(fixed_delay_retry=10,backoff_factor=0)
        self.http_exp_backoff_session : network.requests.Session = network.create_http_session(backoff_factor=1)

    ####SNDs' etl specific formats

    DATE_FORMAT = "%d/%m/%Y"
    DEB_ASSED_CODE_COLUM_NAME = "asset_code"

    # This is the HTML table class used to fetch all of the debentures from a company in os.getenv('LIST_COMPANY_DEBENTURES_URL')
    LIST_DEBS_HTML_TABLE_CLASS = 'Tab10333333' 

    DEB_TERMS_COLUMNS_MAP = {
    # Required fields
    'Serie Emissao': 'series',
    'Data de Emissao': 'issue_date',
    'Data de Vencimento': 'maturity',
    'indice': 'indexer',
    'Juros Criterio Novo - Taxa': 'coupon',
    'Percentual Multiplicador/Rentabilidade': 'coupon_alt',  # Alternative coupon field
    'Criterio de Calculo': 'base',
    'Tipo de Amortizacao': 'amort_type',
    'Garantia/Especie': 'guarantees',
    }
    
    # Fields that might contain covenant information (for guarantees)
    DEB_TERMS_CONVENANT_COLUMNS = [
        'Resgate Antecipado',
        'Escritura Padronizada',
        'Classe',
    ]

    DEB_TRADED_PRICES_COLUMNS_MAPS = {
        "Data" : "ref_date",
        "Emissor" : "issuer",
        "Código do Ativo" : DEB_ASSED_CODE_COLUM_NAME,
        "Quantidade" : "qty",
        "Número de Negócios" : "amt_of_trades",
        "PU Mínimo" : "min_unit_price",
        "PU Médio"  : "avg_unit_price",
        "PU Máximo" : "max_unit_price",
        "% PU da Curva" : "curve_price_percent"
    }

    DEB_EVENTS_SCHEDULE_COLUMNS_MAPS = {
        "Data do Evento" : "event_date",
        "Data do Pagamento" : "payment_date",
        "Emissor" : "issuer",
        "Ativo" : DEB_ASSED_CODE_COLUM_NAME,
        "Evento" : "event",
        "Tipo"  : "yield_type",
        "Taxa/Percentual" : "rate_or_percent",
        "Liquidação" : "settlement_date"
    }

    DEB_PRICES_FLOAT_COLUMNS = ["min_unit_price","avg_unit_price","max_unit_price","curve_price_percent"]
    DEB_EVENTS_SCHEDULE_FLOAT_COLUMNS = ["rate_or_percent"]

    ####

    ####Helper functions
    def list_debentures_from_company(self):

        raw_cnpj = self.config.get_company_cnpj_digits_only()

        url = os.getenv("LIST_COMPANY_DEBENTURES_URL")

        payload=f"""op_exc=False&mnome={raw_cnpj}&ativo=&IPO=&icvm=&EscrituraPadronizada=&
                    cvm_ini=&cvm_fim=&emis_ini=&emis_fim=&venc_ini=&venc_fim=&
                    TPV=&TNV=&rent_ini=&rent_fim=&distrib_ini=&d
                    istrib_fim=&indice=&tipo=&crit_calc=&dia_ref=&
                    mult_rend=&limite=&trat_limite=&tx_spread=&prazo=&
                    premio_novo=&premio_prazo=&premio_antigo=&Par=&
                    amortizacao=&mbanco=&magente=&instdep=&coordenador=&Submit.x=24&Submit.y=11
                """

        headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.debentures.com.br',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
        }

        response = self.http_exp_backoff_session.request("POST", url, headers=headers, data=payload)

        response.raise_for_status()

        html_content = response.content.decode(fmts.SND_ENCODING)
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        debentures = []
        
        result_tables = soup.find_all('table', class_=self.LIST_DEBS_HTML_TABLE_CLASS)
        
        for table in result_tables:
            rows = table.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                
                # Each row should have: empty, ativo link, emissor, empty, situacao, empty
                if len(cols) >= 5:
                    try:
                        # Extract Ativo (code)
                        active_cell = cols[1]
                        active_link = active_cell.find('a')
                        active = active_link.get_text(strip=True) if active_link else ''
        
                        # Extract Emissor (issuer)
                        issuer = cols[2].get_text(strip=True)
                        
                        # Extract Situação (status)
                        situation = cols[4].get_text(strip=True)
                        
                        if active:  # Only add if we have a valid code
                            debentures.append({
                                self.DEB_ASSED_CODE_COLUM_NAME: active,
                                'issuer': issuer,
                                'situation': situation
                            })
                    
                    except Exception as e:
                        continue
        
        df = pd.DataFrame(debentures)
        
        if not df.empty:
            df[self.DEB_ASSED_CODE_COLUM_NAME] = df[self.DEB_ASSED_CODE_COLUM_NAME].str.strip()
            df['issuer'] = df['issuer'].str.strip()
            df['situation'] = df['situation'].str.strip()

        assert not df.empty, self.config.logger.error("Could not parse the listing of debentures for the company. Check if this company has debentures listed in the datasource. Aborting execution...")

        return df

    def get_financial_events_schedule_df(self):
        """
        Function responsible for retrieving a given company's debenture's events schedules
        """
        raw_cnpj = self.config.get_company_cnpj_digits_only()

        url = os.getenv("DEB_FINANCIAL_EVENTS_URL").replace(fmts.PLACEHOLDER_VALUE,raw_cnpj)

        payload=f'emissor={raw_cnpj}&ativo=&evento=&dt_ini=&dt_fim=&dt_pgto_ini=&dt_pgto_fim=&Submit32.x=34&Submit32.y=15'
        headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.debentures.com.br',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
        }

        response = self.http_exp_backoff_session.request("POST", url, headers=headers, data=payload)

        ref_date = fmts.create_ref_date(datetime.today())
        
        save_path = self.config.get_bucket_save_file_path(
                                                          fmts.MedallionLayer.BRONZE_LANDING,
                                                          self.data_source,
                                                          fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                          file_extension="tsv",
                                                          ref_date=ref_date)
        
        self.config.logger.info(f"Saving extracted debenture events schedule to '{save_path}'...")

        self.config.minio_handler.save_file_to_bucket(
                                            save_path,
                                            BytesIO(response.content),
                                            fmts.create_ingest_ts(),
                                            self.config.trace_id,
                                            fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                            fmts.ContentTypes.TSV,
                                            self.data_source
                                            )
        
        self.config.logger.info(f"Saved to bronze/landing with success...")

        events_schedule_df = pd.read_csv(
                            BytesIO(response.content),
                            sep=fmts.SND_TSV_SEPARATOR,
                            encoding=fmts.SND_ENCODING,
                            skiprows=fmts.SND_FINANCIAL_EVENTS_TSV_SKIP_ROWS
                            )

        expected_cols = ["Data do Evento","Data do Pagamento","Emissor","Ativo","Evento","Tipo","Taxa/Percentual","Liquidação"]

        assert events_schedule_df.columns.to_list() == expected_cols, self.config.logger.error("The parsed debenture events schedule did not have the expected columns. Aborting etl...")

        #When the request is valid but an invalid cnpj is passed, it returns the df with 1 row noting that no events schedule were found
        assert events_schedule_df.shape[0] > 1, self.config.logger.error("No events schedule were found for the passed CNPJ. Please double check the passed CNPJ in the .env file. Aborting etl...")

        ref_date=datetime.today().strftime(fmts.DateTimeFormats.FORMATTED_DATE_ONLY.value)

        save_path = self.config.get_bucket_save_file_path(
                                                          fmts.MedallionLayer.BRONZE_RAW,
                                                          self.data_source,
                                                          fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                          file_extension="parquet",
                                                          ref_date=ref_date)
        
        self.config.logger.info(f"Saving parsed DF as parquet to '{save_path}'...")

        self.config.minio_handler.save_file_to_bucket(
                                            save_path,
                                            BytesIO(response.content),
                                            fmts.create_ingest_ts(),
                                            self.config.trace_id,
                                            fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                            fmts.ContentTypes.PARQUET,
                                            self.data_source
                                            )
        
        self.config.logger.info(f"Saved parquet to bronze/raw with success...")

        events_schedule_df = events_schedule_df.rename(columns=self.DEB_EVENTS_SCHEDULE_COLUMNS_MAPS)
        events_schedule_df = fmts.convert_brazilian_numbers_to_float(events_schedule_df,["rate_or_percent"])
        events_schedule_df = events_schedule_df.astype(fmts.DocumentSchemas.DEBENTURE_AGENDA_EVENTOS.value)

        events_schedule_df = events_schedule_df.rename(columns=self.DEB_EVENTS_SCHEDULE_COLUMNS_MAPS)

        return events_schedule_df

    def clean_deb_terms_df(self, df: pd.DataFrame) -> pd.DataFrame:
            """
            Clean and convert data types from debenture terms dataframes
            
            Parameters:
            -----------
            df : DataFrame
                DataFrame to clean
            
            Returns:
            --------
            Cleaned DataFrame
            """
            # Convert dates
            date_columns = ['issue_date', 'maturity']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')
            
            # Convert numeric fields
            numeric_columns = ['coupon']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Clean string fields
            string_cols = df.select_dtypes(include=['string','object'])
            for col in string_cols:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', '-', ''], pd.NA)

            return df

    def clean_deb_terms_column_names(self, name: str) -> str:
        """
        Convert debenture terms column names to snake_case and also removes special characters.
        This is done because the data source provides the columns with spaces, special characters and accents.
        
        Parameters:
        -----------
        name : str
            Original column name
        
        Returns:
        --------
        Cleaned column name in snake_case
        
        Examples:
        ---------
        "Codigo do Ativo" -> "codigo_do_ativo"
        "Data de Emissao" -> "data_de_emissao"
        "Serie Emissao" -> "serie_emissao"
        "Garantia/Especie" -> "garantia_especie"
        "Ato Societario (1)" -> "ato_societario_1"
        "Quantidade  Cancelada" -> "quantidade_cancelada"
        """
        # Convert to lowercase
        name = name.lower()
        
        # Replace Portuguese special characters
        replacements = {
            'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a',
            'é': 'e', 'ê': 'e',
            'í': 'i',
            'ó': 'o', 'ô': 'o', 'õ': 'o',
            'ú': 'u', 'ü': 'u',
            'ç': 'c'
        }
        
        for old, new in replacements.items():
            name = name.replace(old, new)
        
        # Replace special characters with underscore or remove
        name = re.sub(r'[/\(\)\-]', '_', name)  # Replace /, (, ), - with _
        name = re.sub(r'[^\w\s]', '', name)      # Remove other special chars
        name = re.sub(r'\s+', '_', name)         # Replace spaces with _
        name = re.sub(r'_+', '_', name)          # Replace multiple _ with single _
        name = name.strip('_')                   # Remove leading/trailing _
        
        return name

    def get_treated_debenture_terms(self, debenture_code: str) -> dict:
            """
            Scrape debenture terms from debentures.com.br
            
            Parameters:
            -----------
            debenture_code : str
                Debenture code (e.g., 'AMBP16')
            
            Returns:
            --------
            dict with debenture terms
            """
            url = os.getenv("DEB_TERMS_URL").replace(fmts.PLACEHOLDER_VALUE,debenture_code)

            session = self.http_fixed_time_session

            response = session.get(url)
            response.raise_for_status()

            ref_date = fmts.create_ref_date(datetime.today())

            save_path = self.config.get_bucket_save_file_path(
                                                            fmts.MedallionLayer.BRONZE_RAW,
                                                            self.data_source,
                                                            fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                            file_extension="tsv",
                                                            ref_date=ref_date)
            
            self.config.logger.info(f"Saving extracted debenture terms schedule to '{save_path}'...")

            self.config.minio_handler.save_file_to_bucket(
                                                save_path,
                                                BytesIO(response.content),
                                                fmts.create_ingest_ts(),
                                                self.config.trace_id,
                                                fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                fmts.ContentTypes.TSV,
                                                self.data_source
                                                )
            
            self.config.logger.info("File saved with success")

            deb_terms_df = pd.read_csv(
                            BytesIO(response.content),
                            sep=fmts.SND_TSV_SEPARATOR,
                            encoding=fmts.SND_ENCODING,
                            skiprows=fmts.SND_DEB_TERMS_TSV_SKIP_ROWS
                            )
            
            mapped_columns = set()
            result = pd.DataFrame()
            
            # Apply standard mappings
            for original_col, standard_col in self.DEB_TERMS_COLUMNS_MAP.items():
                if original_col in deb_terms_df.columns:
                    result[standard_col] = deb_terms_df[original_col]
                    mapped_columns.add(original_col)
            
            # Handle coupon field (use primary or alternative)
            if 'coupon' not in result.columns or result['coupon'].isna().all():
                if 'coupon_alt' in result.columns:
                    result['coupon'] = result['coupon_alt']
            
            # Remove coupon_alt if it exists
            if 'coupon_alt' in result.columns:
                result = result.drop('coupon_alt', axis=1)
            
            # Create covenants field by combining related fields
            covenant_parts = []
            for col in self.DEB_TERMS_CONVENANT_COLUMNS:
                if col in deb_terms_df.columns:
                    covenant_parts.append(f"{col}: {deb_terms_df[col].fillna('N/A')}")
                    mapped_columns.add(col)
            
            if covenant_parts:
                result['covenants'] = deb_terms_df.apply(
                    lambda row: '; '.join([
                        f"{col}: {row[col]}" 
                        for col in self.DEB_TERMS_CONVENANT_COLUMNS 
                        if col in deb_terms_df.columns and row[col]
                    ]), 
                    axis=1
                )
            else:
                result['covenants'] = ''
            
            # Add all remaining columns with 'enr_' prefix
            for col in deb_terms_df.columns:
                if col not in mapped_columns:
                    # Convert column name: remove spaces, convert to lowercase
                    clean_name = self.clean_deb_terms_column_names(col)
                    result[f'enr_{clean_name}'] = deb_terms_df[col]
            
            # Data type conversions and cleaning
            result = self.clean_deb_terms_df(result)
            
            return result


    def get_treated_deb_traded_prices(self) -> pd.DataFrame:
        """
        Downloads the TSV file from debentures.com.br and treats the contents in order to get the company's debenture's trading prices

        Returns:

            pd.DataFrame : Dataframe containing the treated traded debenture prices for this compay
        """

        digits_only_company_cnpj = self.config.get_company_cnpj_digits_only()
                
        url = os.getenv("DEB_TRADED_PRICES_URL").replace(fmts.PLACEHOLDER_VALUE,digits_only_company_cnpj)

        payload=f'op_exc=False&emissor={digits_only_company_cnpj}4&ativo=&ISIN=&dt_ini=&dt_fim=&Submit32.x=32&Submit32.y=19'
        headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.debentures.com.br',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
        }

        session = self.http_fixed_time_session

        response = session.post(url, headers=headers, data=payload)

        tsv_bytes = BytesIO(response.content)

        traded_prices_df = pd.read_csv(
                                        tsv_bytes,
                                        sep=fmts.SND_TSV_SEPARATOR,
                                        skiprows=fmts.SND_DEB_TRADED_PRICES_TSV_SKIP_ROWS,
                                        encoding=fmts.SND_ENCODING
                                    )
        
        #When the request is valid but an invalid cnpj is passed, it returns the df with 1 row noting that no events schedule were found
        assert traded_prices_df.shape[0] > 1, self.config.logger.error("No traded prices were found for the passed CNPJ. Please double check the passed CNPJ in the .env file. Aborting etl...")

        ref_date = fmts.create_ref_date(datetime.today())

        save_path = self.config.get_bucket_save_file_path(
                                                          fmts.MedallionLayer.BRONZE_LANDING,
                                                          self.data_source,
                                                          fmts.DocumentTypes.DEBENTURE_PRECOS_DE_NEGOCIACAO_MERCADO_SECUNDARIO,
                                                          file_extension="tsv",
                                                          ref_date=ref_date)
        
        self.config.logger.info(f"Saving extracted traded prices to '{save_path}'...")

        self.config.minio_handler.save_file_to_bucket(
                                            save_path,
                                            BytesIO(response.content),
                                            fmts.create_ingest_ts(),
                                            self.config.trace_id,
                                            fmts.DocumentTypes.DEBENTURE_PRECOS_DE_NEGOCIACAO_MERCADO_SECUNDARIO,
                                            fmts.ContentTypes.TSV,
                                            self.data_source
                                            )
        
        self.config.logger.info("Treating the obtained trade prices dataframe...")

        traded_prices_df = traded_prices_df.rename(columns=self.DEB_TRADED_PRICES_COLUMNS_MAPS)

        traded_prices_df = fmts.convert_brazilian_numbers_to_float(traded_prices_df, self.DEB_PRICES_FLOAT_COLUMNS)

        return traded_prices_df

    ####

    ###Full ETL functions

    def deb_financial_events_schedule_full_etl(self) -> List[str]:
        f"""
        Extract the financial events schedule (Agenda de Eventos Financeiros) for all of the company's debentures from debentures.com.br.
        The parquet output is saved at gold/serving and gold/export as '{fmts.GoldServingTableNames.DEB_EVENTS_SCHEDULE.value}-ref_date'
                
        Return:
            List[str]: List of all file paths uploaded to minio bucket. 
        """

        try:

            self.config.logger.info("Starting the ETL for the debenture's financial events schedule...")

            events_df = self.get_financial_events_schedule_df()

            #This is done because the 'tipo' column comes duplicated from the datasource
            events_df["yield_type"] = events_df["yield_type"].apply(lambda x: x[:len(x)//2])
            events_df = events_df.sort_values(by="payment_date")
            string_cols = events_df.select_dtypes(include=['string','object'])
            for col in string_cols:
                events_df[col] = events_df[col].astype(str).str.strip()
                events_df[col] = events_df[col].replace(['nan', 'None', '-', ''], pd.NA)

            events_df = self.config.enforce_dataframe_schema(events_df,
                                                                    fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                                    self.DEB_EVENTS_SCHEDULE_FLOAT_COLUMNS
                                                            )

            self.config.logger.info("Debenture events dataframe parsed and column 'yield_type' treated with sucess. Moving schedule to gold/serving and gold/export")
            
            ref_date=datetime.today().strftime(fmts.DateTimeFormats.FORMATTED_DATE_ONLY.value)
            
            uploaded_files_path = self.config.save_df_to_gold_export_and_serving(
                                                                                    events_df,
                                                                                    self.data_source,
                                                                                    fmts.DocumentTypes.DEBENTURE_AGENDA_EVENTOS,
                                                                                    ref_date,
                                                                                    fmts.GoldServingTableNames.DEB_EVENTS_SCHEDULE
                                                                                )
                
            self.config.logger.info("All debenture schedule events moved to gold/serving and gold/export. Finishing financial events schedule ETL with success...")

            self.config.redis_handler.save_to_cache(fmts.GoldServingTableNames.DEB_EVENTS_SCHEDULE,
                                        events_df.to_dict(orient="records"),
                                        datetime.today(),
                                        self.config.trace_id)

            gold_export_file_path = [uploaded_files_path[fmts.MedallionLayer.GOLD_EXPORT]]

            return gold_export_file_path
        
        except Exception as e:

            raise Exception(f"SNDETL:deb_financial_events_schedule_full_etl: {e}")

    def deb_terms_full_etl(self) -> List[str]:
        f"""
        Extract the debenture terms (Caracteristicas) for all of the company's debentures from debentures.com.br.
        The parquet output is saved at gold/serving and gold/export as '{fmts.GoldServingTableNames.DEB_TERMS.value}-ref_date' 
                
        Return:
            List[str]: List of all file paths uploaded to minio bucket.
        """

        try:

            self.config.logger.info("Listing all debentures from company...")

            company_debs_df = self.list_debentures_from_company()

            self.config.logger.info(f"The following debenture's data were found (listing only first 10 entries): {company_debs_df.head(10)}")
            
            company_debenture_codes = company_debs_df[self.DEB_ASSED_CODE_COLUM_NAME].to_list()

            self.config.logger.info(f"Debentures found for current company: {company_debenture_codes}")

            self.config.logger.info("Starting to get the debenture's terms...")

            ref_date = fmts.create_ref_date(datetime.today())
            debs_amt = len(company_debenture_codes)

            deb_dfs_list = list()

            for idx,curr_asset_code in enumerate(company_debenture_codes):

                self.config.logger.info(f"Getting terms for debenture '{curr_asset_code}'. Progress: {idx}/{debs_amt}")

                debenture_terms_df = self.get_treated_debenture_terms(curr_asset_code)

                self.config.logger.info(f"terms for debenture '{curr_asset_code}' fetched and treated with success.")

                deb_dfs_list.append(debenture_terms_df)

                self.config.logger.info(f"Debenture '{curr_asset_code}' terms processed with success...")

            final_consolidated_df = pd.concat(deb_dfs_list,ignore_index=True)

            #Convert dtype 'object' columns to string to allow for parquet storage
            final_consolidated_df = final_consolidated_df.astype({col: "string" for col in final_consolidated_df.select_dtypes(include="object").columns})
            
            uploaded_files_path = self.config.save_df_to_gold_export_and_serving(   
                                                                                    final_consolidated_df,
                                                                                    self.data_source,
                                                                                    fmts.DocumentTypes.DEBENTURE_CARACTERISTICAS,
                                                                                    ref_date,
                                                                                    fmts.GoldServingTableNames.DEB_TERMS
                                                                                )

            self.config.logger.info(f"All of the debenture terms were processed with success. Starting to move them to gold layer...")

            self.config.redis_handler.save_to_cache(fmts.GoldServingTableNames.DEB_TERMS,
                                final_consolidated_df.to_dict(orient="records"),
                                datetime.today(),
                                self.config.trace_id)

            gold_export_file_path = [uploaded_files_path[fmts.MedallionLayer.GOLD_EXPORT]]

            return gold_export_file_path
        
        except Exception as e:

            raise Exception(f"SNDETL:deb_terms_full_etl: {e}")

    def deb_traded_prices_full_etl(self) -> List[str]:
        f"""
        Extract the debenture secondary market traded prices (Preços de Negociação) for all of the company's debentures from debentures.com.br.
        The parquet output is saved at gold/serving and gold/export as '{fmts.GoldServingTableNames.DEB_SECONDARY_MARKET_TRADED_PRICES.value}-ref_date' 
        
        Return:
            List[str]: List of all file paths uploaded to minio bucket.
        """
        try:

            self.config.logger.info("Starting ETL to get all of the company's debenture secondary market traded price...")

            self.config.logger.info("Listing all debentures from company...")

            company_debs_df = self.list_debentures_from_company()

            self.config.logger.info(f"The following debenture's data were found (listing only first 10 entries): {company_debs_df.head(10)}")
            
            traded_prices_df = self.get_treated_deb_traded_prices()

            #This warning is issued when at least one of the company's issued debenture's traded prices were not found in the secondary market trading prices
            if len(traded_prices_df[self.DEB_ASSED_CODE_COLUM_NAME].unique()) != len(company_debs_df[self.DEB_ASSED_CODE_COLUM_NAME].unique()):
                self.config.logger.warning(f"""At least one of the companies debentures were not found in the secondary market trading prices.
                                            Company debenture's codes : {company_debs_df[self.DEB_ASSED_CODE_COLUM_NAME].unique()} | 
                                            Company debenture's trading prices found for : {traded_prices_df[self.DEB_ASSED_CODE_COLUM_NAME].unique()}
                                            """)

            self.config.logger.info("All treated debenture's secondary market traded prices obtained with success, moving to gold layer...")

            ref_date = fmts.create_ref_date(datetime.today())

            uploaded_files_path = self.config.save_df_to_gold_export_and_serving(   
                                                                            traded_prices_df,
                                                                            self.data_source,
                                                                            fmts.DocumentTypes.DEBENTURE_PRECOS_DE_NEGOCIACAO_MERCADO_SECUNDARIO,
                                                                            ref_date,
                                                                            fmts.GoldServingTableNames.DEB_SECONDARY_MARKET_TRADED_PRICES
                                                                        )
            
            self.config.logger.info("ETL for the secondary market trade prices of the debenture's finished with success...")

            self.config.redis_handler.save_to_cache(fmts.GoldServingTableNames.DEB_SECONDARY_MARKET_TRADED_PRICES,
                                traded_prices_df.to_dict(orient="records"),
                                datetime.today(),
                                self.config.trace_id)


            gold_export_file_path = [uploaded_files_path[fmts.MedallionLayer.GOLD_EXPORT]]

            return gold_export_file_path
        
        except Exception as e:

            raise Exception(f"SNDETL:deb_traded_prices_full_etl: {e}")






