# Data Inventory

_Last updated: 2025-10-19_

## Usage and core components

Usage instructions:
- Python 3.12 installed on host machine
- Python libs from requirements.txt installed
- Docker and Docker compose
- Models qwen2.5:7b for LLM chat and bge-m3:567m for embeddings configured and running at Ollama
- Ensure that vm.max_map_count ≥ 262144. If not use this command 'sudo sysctl -w vm.max_map_count=262144'
- Confirm that your host machine requirements are enough to run [RagFlow and its dependencies](https://ragflow.io/docs/dev/)
- To run the fast api, execute at this directory the script 'run_api.sh'
- To run the ETL processes, run 'etl_orchestrator.py'
- To import RagFlow's configs, follow these [instructions](https://ragflow.io/docs/dev/guides/migration/migrate_from_docker_compose). The path containing the tar backups is /ragflow. Use the script /docker/migration.sh to restore the backup

Handlers:
- MinioHandler - Class responsible for interacting with the MinIO bucket
- RagFlowHandler - Class responsible for interacting with the RagFlow datasets, agents and file parsing
- LLMHandler - Class responsible for interacting with the local LLM model hosted in Ollama
- RedisHandler - Class responsible for interacting with the Redis cache

ETL:
- CVM:
    - FS_full_etl           : Method responsible for ingesting and treating the ITR ('Informações Trimestrais') and DFP ('Demonstrações Financeiras Padronizadas') documents
    - FRE_full_etl          : Method responsible for downloading the FRE ('Formulário de Referência') documents
    - IPE_full_etl          : Method responsible for downloading the IPE ('Periódicos e Eventuais') documents
    - RAD_document_full_etl : Method responsible for downloading documents from CVM's RAD

- SND:
    - deb_financial_events_schedule_full_etl : Method responsible for downloading the events schedule for the debentures
    - deb_terms_full_etl                     : Method responsible for downloading the debenture terms
    - deb_traded_prices_full_etl             : Method responsible for downloading the debenture traded prices

- B3:
    - macro_data_full_etl : Method responsible for downloading macro data (rates and FX)

- Other modules:
    - formtas.py : Module responsible for consolidating the formats and unifying processes to guarantee consistence across different modules
    - network.py : Module to handle network related matters such as the standardized creation of HTTP sessions respecting exponential backoffs or fixed with delays
    - logger:py : Responsible for implementing the logging solution, saving to the stdout and also to a log file. They are identifiable by trace_id
    - file_operations.py : Module responsible for unifying and centralizing all file operations
    - api.py : Api gateway responsible for serving gold layer data

##  Data types and sources

|Data type| Data Source|URL|Periodicitiy|Licenses|Restrictions
|-|-|-|-|-|-|
|ITR|Portal Dados Abertos CVM|[URL](https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr) | Weekly | ODbL | [Robots.txt](#4.1-licensing-and-compliances) [1]
|DFP|Portal Dados Abertos CVM|[URL](https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp) | Weekly | ODbL | [Robots.txt](#restrictions) [1]
|FRE|Portal Dados Abertos CVM|[URL](https://dados.cvm.gov.br/dataset/cia_aberta-doc-fre) | Weekly | ODbL | [Robots.txt](#restrictions) [1]
|IPE|Portal Dados Abertos CVM|[URL](https://dados.cvm.gov.br/dataset/cia_aberta-doc-ipe) | Weekly, referring to Y-1 | ODbL | [Robots.txt](#restrictions) [1]
|Press release|RAD CVM|[URL](https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx/ListarDocumentos) | Not disclosed | Not disclosed | Not disclosed
|Relatório de agência de rating|RAD CVM|[URL](https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx/ListarDocumentos) | Not disclosed | Not disclosed | Not disclosed
|Notificação do agente fiduciário aos debenturistas|RAD CVM|[URL](https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx/ListarDocumentos) | Not disclosed | Not disclosed | Not disclosed
|Agenda de eventos financeiros de debêntures|SND|[URL](https://www.debentures.com.br/exploreosnd/consultaadados/eventosfinanceiros/agenda_f.asp) | Not disclosed | Not disclosed | Not disclosed
|Preços de negociação no mercado secundário|SND|[URL](https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_f.asp?op_exc=False&emissor=&ISIN=&ativo=) | Not disclosed | Not disclosed | Not disclosed
|Características das debêntures públicas|SND|[URL](https://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/caracteristicas_d.asp?tip_deb=publicas&selecao=AMBP15) | Not disclosed | Not disclosed | Not disclosed
|Indicadores financeiros|B3|[URL](https://sistemaswebb3-derivativos.b3.com.br/financialIndicatorsProxy/FinancialIndicators/GetFinancialIndicators/eyJsYW5ndWFnZSI6InB0LWJyIn0=) | Daily for FX and SOFR, D-1 for domestic rates | Not disclosed | Not disclosed

---

## Gold layer schemas

- ***Table: financials_quarterly.parquet***

    **Sources:**
            [ITR](#data-types-and-sources) and [DFP](#data-types-and-sources)
    
    **Description:**
                Table responsible for storing financial statements data relevant to the company. 
                The ITR and DFP document are joined to create the finalized version of the income statement.

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: DEMONSTRATIVOS_FINANCEIROS_FINAL
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: CVM
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema** 

    #### **Core Identifiers**       

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | issuer_cnpj | string | Formatted company CNPJ | 12.648.266/0001-24 | Primary identifier for issuer |
    | date | datetime | Date that the information is linked to | 2025-10-14 | Reference date for metrics |
    | quarter | float | Year quarter | Q2 | Fiscal quarter (Q1-Q4) |
    | year | int | Data's year | 2025 | Fiscal year |

    #### **Income Statement Metrics**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | revenue | float64 | Company's reported revenue in BRL | 1000000 | Top-line revenue from operations |
    | ebitda | float64 | Company's calculated EBITDA in BRL | 1000000 | Earnings before interest, taxes, depreciation & amortization |
    | ebit | float64 | Company's calculated EBIT in BRL | 1000000 | Operating income before interest & taxes |
    | depreciation | float64 | Company's calculated depreciation in BRL | 1000000 | D&A expense |
    | interest_paid | float64 | Company's interest paid in BRL | 1000000 | Interest expense on debt |

    #### **Balance Sheet Metrics - Assets**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | cash | float64 | Company's cash in BRL | 1000000 | Cash and cash equivalents |

    #### **Balance Sheet Metrics - Liabilities**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | net_debt | float64 | Company's net debt in BRL | 1000000 | Total debt minus cash |
    | total_debt | float64 | Company's total debt in BRL | 1000000 | Short-term + long-term debt |
    | debt_short_term | float64 | Company's short term debt in BRL | 1000000 | Debt maturing within 1 year |
    | debt_long_term | float64 | Company's long term debt in BRL | 1000000 | Debt maturing beyond 1 year |

    #### **Cash Flow Metrics**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | capex | float64 | Company's capex in BRL | 1000000 | Capital expenditures on fixed assets |
    | wc_change | float64 | Company's working capital changes in BRL | 1000000 | Change in net working capital |


  <br>
  <br>
- ***Table: debentures_terms.parquet***

    **Sources:**
            [SND](#data-types-and-sources) 
    
    **Description:**
                Table responsible for storing the debentures terms and characteristics. 

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: DEB_CARACTERISTICAS
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: SND
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema**

    #### **Core Identifiers**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_codigo_do_ativo | string | Debenture ticker code | AMBP15 | Primary identifier for trading |
    | enr_isin | string | International Securities Identification Number | BRAMBPDBS040 | Global unique identifier |
    | enr_cnpj | string | Issuer CNPJ (without formatting) | 12.648.266/0001-24 | Company tax ID |
    | enr_empresa | string | Full legal name of issuing company | AMBIPAR PARTICIPACOES E EMPREENDIMENTOS S/A | Issuer name |

    #### **Series & Issue Information**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | series | string | Series identifier | UNI 5 | Unique series within issuer |
    | enr_serie | string | Original series field | UNI 5 | Same as series |
    | enr_emissao | string | Emission number | 5 | Sequential emission number |
    | issue_date | datetime64 | Date of debenture issuance | 2023-04-17 | When securities were issued |
    | enr_situacao | category | Current status of debenture | Registrado | Status: Registrado, Cancelado, Vencido |
    | enr_ipo | string | IPO indicator | - | Initial public offering flag |

    #### **Regulatory Information**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_registro_cvm_da_emissao | string | CVM registration number for emission | SREAUTDEBPRI2023083 | Securities regulator registration |
    | enr_data_de_registro_cvm_da_emissao | datetime64 | CVM registration date | 2023-05-02 | When registered with CVM |
    | enr_registro_cvm_do_programa | string | CVM program registration number | - | For shelf programs |
    | enr_data_de_registro_cvm_do_programa | datetime64 | CVM program registration date | - | Program registration date |
    | enr_deb_incent_lei_12431 | category | Tax-incentivized debenture (Lei 12.431) | N | Y/N for tax benefits |

    #### **Maturity & Key Dates**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | maturity | datetime64 | Final maturity date | 2029-04-17 | When principal is fully repaid |
    | enr_data_de_vencimento | datetime64 | Original maturity date field | 2029-04-17 | Same as maturity |
    | enr_data_de_saida_novo_vencimento | datetime64 | Early redemption / new maturity date | 2029-04-17 | If redeemed early |
    | enr_motivo_de_saida | string | Reason for early exit | - | Exit reason if applicable |
    | enr_data_do_inicio_da_rentabilidade | datetime64 | Start date for interest accrual | 2023-05-08 | When interest starts |
    | enr_data_do_inicio_da_distribuicao | datetime64 | Distribution start date | 2023-05-02 | Public offering start |
    | enr_data_da_proxima_repactuacao | datetime64 | Next repricing date | - | For floating rate reset |
    | enr_data_ult_vna | datetime64 | Last updated nominal value date | 2025-10-14 | Last VNA update |

    #### **Corporate Actions**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_ato_societario_1 | category | Corporate action type 1 | RCA | RCA, AGE, AGO, etc. |
    | enr_data_do_ato_1 | datetime64 | Date of corporate action 1 | 2023-04-17 | Action date |
    | enr_ato_societario_2 | category | Corporate action type 2 | - | Second action if any |
    | enr_data_do_ato_2 | datetime64 | Date of corporate action 2 | - | Second action date |

    #### **Form & Structure**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_forma | category | Form of debenture | Escritural | Escritural (book-entry) vs physical |
    | guarantees | category | Type of guarantee/collateral | Quirografária | Quirografária, Real, Flutuante, Subordinada |
    | covenants | string | Covenant information | Resgate Antecipado: S | Combined covenant text |
    | amort_type | string | Amortization schedule type | Percentual variável | Description of amortization method |
    | enr_amortizacao_criterio | string | Amortization criteria | - | Detailed amortization rules |

    #### **Quantities**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_quantidade_emitida | Int64 | Total quantity issued | 300000 | Initial issuance size |
    | enr_quantidade_em_mercado | Int64 | Quantity outstanding in market | 300000 | Currently trading |
    | enr_quantidade_em_tesouraria | Int64 | Quantity held in treasury | 0 | Company buybacks |
    | enr_quantidade_resgatada | Int64 | Quantity redeemed | 0 | Early redemptions |
    | enr_quantidade_cancelada | Int64 | Quantity cancelled | 0 | Cancelled securities |
    | enr_quantidade_convertida_no_snd | Int64 | Quantity converted in SND system | 0 | Conversions tracked by SND |
    | enr_quantidade_convertida_fora_do_snd | Int64 | Quantity converted outside SND | 0 | Other conversions |
    | enr_quantidade_permutada_no_snd | Int64 | Quantity exchanged in SND | 0 | Exchanges in SND |
    | enr_quantidade_permutada_fora_do_snd | Int64 | Quantity exchanged outside SND | 0 | Other exchanges |
    | enr_artigo_14 | Int64 | Restricted placement (Article 14) | - | Securities law restriction |
    | enr_artigo_24 | Int64 | Restricted trading (Article 24) | - | Trading restrictions |

    #### **Nominal Values**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_valor_nominal_na_emissao | float64 | Face value at issuance | 1000 | Initial face value per unit |
    | enr_valor_nominal_atual | float64 | Current face value | 1000 | Current face value (may include indexation) |
    | enr_unidade_monetaria | category | Currency unit | R$ | BRL, USD, etc. |
    | enr_unidade_monetaria1 | category | Alternative currency field | R$ | Duplicate currency field |

    #### **Remuneration Structure**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | indexer | category | Index for inflation adjustment | DI | DI, IPCA, IGPM, PRE, TJLP |
    | coupon | float64 | Spread or fixed rate (% p.a.) | 2.45 | Annual rate in percentage |
    | base | category | Calculation methodology | Padrão - SND | SND standard or other |
    | enr_tipo | category | Type classification | - | Fixed, floating, etc. |
    | enr_criterio_para_indice | string | Index calculation criteria | - | How index is applied |
    | enr_corrige_a_cada | string | Indexation frequency | - | How often indexed |
    | enr_dia_de_referencia_para_indice_de_precos | string | Reference day for price index | - | Day of month for IPCA/IGPM |

    #### **Interest Payment Terms**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_juros_criterio_novo_prazo | Int64 | Interest payment tenor (total periods) | 252 | Total number of payments |
    | enr_juros_criterio_novo_cada | Int64 | Interest payment frequency | 6 | Every N periods |
    | enr_juros_criterio_novo_unidade | category | Interest frequency unit | MES | DIA, MES, ANO |
    | enr_juros_criterio_novo_carencia | string | Interest grace period | 2024-06-12 | First payment date |
    | enr_juros_criterio_novo_criterio | category | Interest calculation criteria | Útil | Útil (business days) vs calendar |
    | enr_juros_criterio_novo_tipo | category | Interest type | Exponencial | Exponencial, Linear |
    | enr_juros_criterio_antigo_do_snd | string | Legacy SND interest criteria | - | Old SND methodology |

    #### **Amortization Terms**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_amortizacao_taxa | float64 | Amortization rate/percentage | - | % of principal per payment |
    | enr_amortizacao_cada | Int64 | Amortization frequency | 12 | Every N periods |
    | enr_amortizacao_unidade | category | Amortization frequency unit | MES | DIA, MES, ANO |
    | enr_amortizacao_carencia | string | Amortization grace period | 2027-04-25 | First amortization date |

    #### **Premium/Bonus Terms**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_premio_criterio_novo_taxa | float64 | Premium rate | - | Bonus/premium percentage |
    | enr_premio_criterio_novo_prazo | Int64 | Premium payment periods | - | Total premium payments |
    | enr_premio_criterio_novo_cada | Int64 | Premium frequency | - | Every N periods |
    | enr_premio_criterio_novo_unidade | category | Premium frequency unit | - | DIA, MES, ANO |
    | enr_premio_criterio_novo_carencia | string | Premium grace period | - | First premium date |
    | enr_premio_criterio_novo_criterio | string | Premium calculation criteria | - | How premium calculated |
    | enr_premio_criterio_novo_tipo | string | Premium type | - | Premium structure type |
    | enr_premios_criterio_antigo_do_snd | string | Legacy SND premium criteria | - | Old methodology |

    #### **Profit Sharing Terms**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_participacao_taxa | float64 | Profit sharing rate | - | % of profits shared |
    | enr_participacao_cada | Int64 | Profit sharing frequency | - | Every N periods |
    | enr_participacao_unidade | category | Profit sharing unit | - | DIA, MES, ANO |
    | enr_participacao_carencia | string | Profit sharing grace period | - | When starts |
    | enr_participacao_descricao | string | Profit sharing description | - | Details of sharing mechanism |

    #### **TJLP Terms (Government Rate)**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_limite_da_tjlp | float64 | TJLP limit/cap | - | Maximum TJLP rate |
    | enr_tipo_de_tratamento_do_limite_da_tjlp | string | TJLP limit treatment type | - | How limit is applied |

    #### **Agents & Intermediaries**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_banco_mandatario | string | Paying agent bank | - | Bank handling payments |
    | enr_agente_fiduciario | string | Fiduciary agent | OLIVEIRA TRUST DTVM S/A | Bondholder trustee |
    | enr_instituicao_depositaria | string | Depository institution | OLIVEIRA TRUST DTVM S/A | Securities custodian |
    | enr_coordenador_lider | string | Lead underwriter | UBS BB CCTVM S/A | Main placement agent |

<br>
<br>

- ***Table: debentures_financial_events_schedule.parquet***

    **Sources:**
            [SND](#data-types-and-sources)
    
    **Description:**
                Table responsible for storing the financial events schedule relevant to the debentures. 

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: DEB_AGENDA_EVENTOS 
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: SND
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema** 

    #### **Core Identifiers**       

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | issuer | string | Formatted company CNPJ | 12.648.266/0001-24 | Primary identifier for issuer |
    | asset_code | datetime | Debenture asset code | AMBP15| Primary identifier for a given debenture |

    #### **Additional data**
    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | event_date | datetime | Date that the given event ocurred | 2025-10-14 | Reference date for the event |
    | payment_date | datetime | Date that the event is going to be paid | 2025-10-14 | May differ from event date because payments can only happen in business days |
    | event | string | Event type  | 'Juros' or 'Amortização' | Reference date for the event |
    | yield_type | string | Describes the yield of this event  | DI | Refers to how will this event's payment be yielded |
    | rate_or_percent | float | Rate or percent of the yield type  | DI | Illustrates how will this event pay  |
    | settlement_date | datetime | Indicates the liquidation date  | DI | Day when cash is actually transferred  |

<br>
<br>

- ***Table: debentures_secondary_market_traded_prices.parquet***

    **Sources:**
            [SND](#data-types-and-sources)
    
    **Description:**
                Table responsible for storing information related to the secondary market traded price. 

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: DEB_PRECOS_DE_NEGOCIACAO_MERCADO_SECUNDARIO 
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: SND
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema** 

    #### **Core Identifiers**       

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | issuer | string | Formatted company CNPJ | 12.648.266/0001-24 | Primary identifier for issuer |
    | asset_code | datetime | Debenture asset code | AMBP15| Primary identifier for a given debenture |
    | ISIN | string | Debenture ISIN code | BRAMBPDBS065 | International Securities Identification Number |

    #### **Additional data**
    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | ref_date | datetime | Trading date | 2025-10-14 | Date that the trade was done |
    | qty | int | Quantity of units that were traded | 10000 | Amount of units that exchanged hands |
    | amt_of_trades | int | Trades amount  | 20 | Number of trades |
    | min_unit_price | float64 | Minimum traded unit price in this trading session  | 130.848 | The lowest trading price on that day |
    | min_unit_price | float64 | Average of traded unit price in this trading session  | 130.848 | The average of the trading prices on that day |
    | min_unit_price | float64 | Maximum traded unit price in this trading session  | 130.848 | The highest trading price on that day |
    | rate_or_percent | float | Rate or percent of the yield type  | DI | Illustrates how will this event pay  |
    | curve_price_percent | float | Percentage deviation from the theoretical curve price | 50.82 | Percentage deviation from the theoretical curve price  |

<br>
<br>

- ***Table: macro_data.parquet***

    **Sources:**
            [B3](#data-types-and-sources)
    
    **Description:**
                Table responsible for storing macro economic data such as FX and domestic/international rates. 

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: B3_DADOS_MACRO 
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: SND
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema** 

    #### **Core Identifiers**       

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | security_id | string | B3's security identifier | 10014464 | Primary identification for a given B3 security |

    #### **Additional data**
    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | description | string | Description of the  given security | 'TAXA SELIC' | Brief text description of what is the security |
    | type | string | Type mapping of the security | 'FX', 'INTERNATIONAL_RATES' or 'DOMESTIC_RATES' | Enumerate of the possible security types |
    | ref_date | datetime | Date that the row information is valid | 2025-10-14 | Refers to the date the the security is linked to |

<br>
<br>

- ***Table: litigation_events.parquet***

    **Sources:**
        [FRE](#data-types-and-sources)
    
    **Description:**
        Table responsible for storing the public judicial legal proceedings provided in the FRE document 'Processos Não Sigilosos'.

    ## **File Metadata**

    - **Content type**: application/octet-stream
    - **X-Amz-Meta-Document_type**: PROCESSOS_JUDICIAIS_NAO_SIGILOSOS
    - **X-Amz-Meta-File_hash**: File SHA256 hash
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Ref_date**: YYYY-MM-DD
    - **X-Amz-Meta-Ingest_ts**: YYYY-MM-DDTHH:MM:SS
    - **X-Amz-Meta-Source**: CVM
    - **X-Amz-Meta-Trace_id**: ETL job execution trace id

    ## **Schema** 

    #### **Core Identifiers**   
    
    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | case_id | string | Unique case number | 0001234-56.2024.8.26.0100 | Primary identifier for legal case |

    #### **Financial & Risk Assessment**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | court | string | Court handling the case | 1ª Vara Cível de São Paulo | Jurisdiction and court instance |
    | case_type | string | Type of legal action | Ação de Indenização | Nature of the lawsuit |
    | class | string | Court instance level | 1ª Instância | First or second instance |
    | stage | string | Current process stage | Sentenciado | Phase in the legal process |
    | last_movement_date | datetime | Date of case filing/last movement | 2024-03-15 | When case was initiated or last updated |
    | claimed_value | float64 | Amount in dispute (BRL) | 500000.00 | Total value claimed in lawsuit |
    | risk | string | Probability of loss assessment | Remoto / Possível / Provável | Risk classification  |

    #### **Enriched Case Details**

    | Column Name | Type | Description | Example | Notes |
    |------------|------|-------------|---------|-------|
    | enr_partes_no_processo | string | Parties involved in the case | Autor: Empresa X; Réu: Empresa Y | Plaintiff and defendant information |
    | enr_principais_fatos | text | Main facts of the case | Alegação de dano material... | Summary of key events and claims |
    | enr_resumo_das_decisoes | text | Summary of court decisions | Sentença procedente... | Key rulings and decisions |
    | enr_relevancia | string | Case relevance assessment | Alta relevância estratégica | Importance classification |
    | enr_impacto_em_caso_de_perda | text | Impact analysis if case is lost | Impacto financeiro significativo... | Potential consequences of adverse ruling |

<br>
<br>
<br>

# Policy Notes

**Effective Date: October 19, 2025**

**Last Updated: October 19, 2025**

### 1. Purpose and Scope
    This Data Policy governs the collection, processing, storage, and usage of financial data within this project. 
    This policy applies to all data sources, datasets, and analytical outputs produced as part of this work.

## 2. Data Sources and Collection Methods
### 2.1 Public Data Sources Only
    All data collected and utilized in this project originates exclusively from publicly accessible sources available on the internet.

    This project does not utilize:

        Proprietary databases
        Data behind paywalls or authentication
        Data from restricted APIs requiring credentials
        Information from private or privileged sources

    The primary data sources include, but are not limited to:

        - CVM
        - SND - Sistema Nacional de Debêntures
        - B3
        - Company's investor relations page

### 2.2 Data Collection Methodology
        Data collection has been performed using the following methods:

            Web Scraping: Automated extraction of publicly displayed information from web pages
            API Access: Use of publicly available APIs where provided

## 3. Data Sensitivity and Responsibility

### 3.1 Public Nature of Data
    As all data sources are publicly accessible without authentication or privileged access, the data collected is considered public information. 
    Any data that should be considered sensitive or confidential is the responsibility of the original data source to protect through appropriate access controls.

### 3.2 No Personally Identifiable Information (PII)
    This project does not intentionally collect, process, or store Personally Identifiable Information (PII). All data relates to:

        Corporate entities (identified by CNPJ - Brazilian corporate tax ID)
        Financial securities (debentures, bonds)
        Aggregate financial metrics
        Market data

### 3.3 Responsibility Disclaimer
    The correct and appropriate handling of sensitive data is the sole responsibility of the original public data sources. 
    
    This project assumes that:

    Data made publicly available by regulatory bodies (CVM, BACEN) has been properly reviewed and cleared for public disclosure
    Any data that should be restricted or confidential is not published on public websites without access controls
    Source platforms have implemented appropriate measures to prevent unauthorized disclosure of sensitive information

    This project takes no responsibility for the sensitivity classification or handling of data at the source level.

## 4. Intellectual Property and Licensing

### 4.1 Licensing and Compliance
    For each data source utilized, the following due diligence has been performed:

        Rate limiting: The project implements exponential backoff and fixed delay request to avoid overloading source servers. 
        Logging: All data ingestion have a trace ID attached, allowing for quick trailing of job executions.
        No unnecessary downloads: All data is checked for updates before downloading. This is done to avoid overloading source servers
        Terms of Use Review: All available terms of use, terms of service, and usage policies have been located and reviewed
        License Verification: Where explicit data licenses exist (e.g., Creative Commons, Open Data License), compliance has been verified
        Attribution: Where required, proper attribution to data sources is maintained in outputs and documentation
        Robots.txt Compliance: All robots.txt directives have been identified and strictly followed, and the found ones are listed below:

            - Portal Dados Abertos CVM - Robots.txt [1]
                User-agent: *
                Disallow: /dataset/rate/
                Disallow: /revision/
                Disallow: /dataset/*/history
                Disallow: /api/
                Crawl-Delay: 10

## 5. Compliance with Laws and Regulations

    This data policy and project are designed to comply with:

        Lei Geral de Proteção de Dados (LGPD) - Brazilian General Data Protection Law (Note: LGPD primarily applies to personal data; this project processes corporate data)

        CVM Regulations - Regulations governing the use of publicly disclosed financial information
        
        Marco Civil da Internet - Brazilian Civil Rights Framework for the Internet


