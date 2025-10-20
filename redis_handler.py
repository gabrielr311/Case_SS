import redis
import formats as fmts
from datetime import datetime, date
from logging import Logger
import json


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


class RedisHandler():
    """
    Class responsible for handling cache operations with Redis
    """
    def __init__(self,
                 logger: Logger,
                 host: str,
                 port: int,
                 password: str):

        self.client = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.logger = logger

    def save_to_cache(self,
                      gold_table  : fmts.GoldServingTableNames,
                      data_dict   : dict,
                      ref_date    : datetime,
                      trace_id    : str,
                      agg_type    : fmts.CVMDocumentAggregationType = None):
        """
        Function responsible for storing data into the redis cache, enforcing the required fields.

        """
        
        self.logger.info(f"Saving data into Redis cache for gold layer table '{gold_table.value}' with reference date '{ref_date}' and trace id '{trace_id}'")
        
        agg_type = f"_{agg_type.value}_" if agg_type else ""

        save_dict = {
                    "data" : data_dict,
                    "file_bucket_path" : f"gold/serving/{gold_table.value}{agg_type}.parquet",
                    "ref_data" : fmts.create_ref_date(ref_date),
                    "trace_id" : trace_id
                }
        
        self.client.set(
                            gold_table.value,
                            json.dumps(save_dict,cls=DateTimeEncoder)
                        )
        
        self.logger.info("Data saved into Redis cache with success")

    def get_from_cache(self,
                       gold_table : fmts.GoldServingTableNames):
        """
        Function to retrieve a given file from the redis cache
        """
        
        return self.client.get(gold_table.value)