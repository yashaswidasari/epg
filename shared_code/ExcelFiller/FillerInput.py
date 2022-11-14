from dataclasses import dataclass
from typing import Dict, List
import pandas as pd
from abc import abstractmethod, ABC


class ServiceMap:
        
    def get_service_params(self, original_svc) -> Dict:
        """
        original_svc is an int for xpotrack service id.
        returns parameters needed for FillerInputModel, including service_name and service_abbr
        """
        raise NotImplementedError

#generated at app init, preload data
class ServiceMapFromExcel(ServiceMap):
    def __init__(self, service_df_path):
        """
        columns must include 'ORIGINAL_SERVICE', 'SERVICE_NAME', 'SERVICE_ABBR'
        """
        service_df = pd.read_excel(service_df_path)
        service_df.columns = [column.lower() for column in service_df.columns]
        self.service_dict = service_df.set_index('original_service').to_dict('index')
        
    def get_service_params(self, original_svc: int) -> Dict:
        service_search = self.service_dict.get(original_svc)
        if not service_search:
            return {'service_name': str(original_svc), 'service_abbr': str(original_svc)}
        return service_search
        

def validate_missing_columns(req_cols, imported_cols):
    missing_cols = [req_col for req_col in req_cols if req_col not in imported_cols]
    if missing_cols:
        raise Exception(f'Columns {missing_cols} are missing, please check initialization process or required columns')
        
        
class BaseRates(ABC):
    cust_rates: pd.DataFrame
    BASE_COLS = ['ORIGINAL_CTY', 'PC_RATE', 'WT_RATE', 'ORIGINAL_SERVICE', 'PRODUCT', 'PC_WT_MIN', 'PC_WT_MAX', 'MAIL_FORMAT', 'MAIL_TYPE']
    
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass
    
    def get_rates(self, original_service) -> pd.DataFrame:
        return (self.cust_rates
                    .query(f'ORIGINAL_SERVICE == {original_service}')
                    [self.BASE_COLS])
    
    def get_grouped_rates(self):
        return self.cust_rates.groupby('ORIGINAL_SERVICE')
    
class BaseRatesFromSnowpark(BaseRates):
    def __init__(self, snowpark_df):
        validate_missing_columns(self.BASE_COLS, snowpark_df.columns)
        self.cust_rates = snowpark_df.to_pandas()
        
class BaseRatesFromPandas(BaseRates):
    def __init__(self, df: pd.DataFrame):
        validate_missing_columns(self.BASE_COLS, df.columns)
        self.cust_rates = df
        

#planned to generate during request?  can't imagine there are too many zones though...
class ZoneMap(ABC):
    cust_zones: pd.DataFrame
    ZONE_COLS = ['ORIGINAL_CTY', 'ORIGINAL_SERVICE', 'ZONE_CODE', 'ZONE_DESCRIPTION', 'ZONE_ORDER', 'COUNTRY_NAME', 'MAX_WT']
    
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass
    
    def get_zone_map(self, original_service):
        return (self.cust_zones
                    .query(f'ORIGINAL_SERVICE == {original_service}')
                    [list(self.ZONE_COLS)])
        
class ZoneMapFromExcel(ZoneMap):
    def __init__(self, zone_map_path):
        cust_zones = pd.read_excel(zone_map_path, dtype=str, keep_default_na=False).astype({'ORIGINAL_SERVICE':int}) #this is the query equivalent, results should already be joined and filtered
        self.cust_zones = cust_zones
        validate_missing_columns(self.ZONE_COLS, cust_zones.columns)
       
        
class Surcharges(ABC):
    cust_surcharges: pd.DataFrame
    SURCHG_COLS = ['ORIGINAL_CTY', 'ORIGINAL_SERVICE', 'PRODUCT', 'SURCHARGE_PC', 'SURCHARGE_LB']
    
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass
    
    def get_surcharges(self, original_service):
        return (self.cust_surcharges
                    .query(f'ORIGINAL_SERVICE == {original_service}')
                    [self.SURCHG_COLS])
    
    def backout_surcharges(self, base_rates: BaseRates):
        pass
    
    
class SurchargesFromExcel(Surcharges):
    def __init__(self, surcharge_path):
        cust_surcharges = pd.read_excel(surcharge_path, dtype=str, keep_default_na=False).astype({'ORIGINAL_SERVICE':int}) #this is the query equivalent, results should already be joined and filtered
        self.cust_surcharges = cust_surcharges
        validate_missing_columns(self.SURCHG_COLS, cust_surcharges.columns)
        
class SurchargesDummy(Surcharges):
    def __init__(self):
        self.cust_surcharges = pd.DataFrame(columns = self.SURCHG_COLS)
        
        
@dataclass
class FillerInputModel:
    base_rates: pd.DataFrame
    zone_map: pd.DataFrame
    service_id: int
    surcharges: pd.DataFrame = None
    service_name: str = None
    service_abbr: str = None
    cust_name: str = ''
    
@dataclass
class QuoteParamsModel:
    cust_name: str
    quote_num: str
    quote_date: str
    custno: str = None


@dataclass
class FillerInputGenerator:
    base_rates: BaseRates
    service_map: ServiceMap
    zone_mapper: ZoneMap
    surcharges: Surcharges
    quote_params: QuoteParamsModel
    
    def split_rates_by_svc(self) -> List[FillerInputModel]:
        return [FillerInputModel(base_rates = service_rates,
                                 zone_map = self.zone_mapper.get_zone_map(service),
                                 service_id = service,
                                 surcharges = self.surcharges.get_surcharges(service),
                                 cust_name = self.quote_params.cust_name,
                                 **self.service_map.get_service_params(service))
                for service, service_rates in self.base_rates.get_grouped_rates()]

    def transform_rates_to_upload(self):
        pass


#utility functions
def snowflake_quote_to_filler_input(quote: pd.DataFrame) -> pd.DataFrame:
    update = (quote.rename(columns={'WEIGHT':'PC_WT_MAX'})
                  .sort_values(['ORIGINAL_SERVICE', 'ORIGINAL_CTY', 'PC_WT_MAX'])
                  .assign(PC_WT_MIN = lambda df: df.groupby(['ORIGINAL_SERVICE', 'ORIGINAL_CTY']).PC_WT_MAX.shift(1).fillna(0.0001)))
    return update

def ppx_to_filler_input(tariff: pd.DataFrame) -> pd.DataFrame:
    update = (tariff.rename(columns={'CTYCODE':'ORIGINAL_CTY'})
                  .assign(MAIL_TYPE = 'PR',
                          MAIL_FORMAT = 'PACK'))
    return update

def xpo_to_filler_input(rates: pd.DataFrame) -> pd.DataFrame:
    update = (rates.rename(columns={'COUNTRYCODE':'ORIGINAL_CTY', 'SERVICEID':'ORIGINAL_SERVICE', 'PCCHARGE':'PC_RATE',
                                    'WEIGHTCHARGE':'WT_RATE', 'MAILTYPE':'MAIL_TYPE', 'MAILFORMAT': 'MAIL_FORMAT'})
                   .assign(PC_WT_MIN = lambda df: df.MINOZ / 16, PC_WT_MAX = lambda df: df.MAXOZ / 16))
    return update