import numpy as np
import pandas as pd
from snowflake.snowpark import Session, Window, DataFrame
from snowflake.snowpark.functions import builtin, rpad, col, lit, left, replace

from shared_code.SnowparkSession import SnowflakeQuoterSession

to_numeric = builtin('to_numeric')
hex_encode = builtin('hex_encode')

def encode_dense_zip_col(column_in, final_len):
    hex_pattern = 'X' * final_len * 2
    return to_numeric(hex_encode(rpad(left(replace(column_in, ' ', ''), final_len), final_len, lit('0'))), hex_pattern)


def sf_upload_excel(snow_session, excel_path, **kwargs):
    df = pd.read_excel(excel_path, keep_default_na=False, **kwargs)
    sf_df = snow_session.session.create_dataframe([[float(val) if type(val) == np.float64 else val for val in row.values] for i, row in df.iterrows()],
                                                  schema=[name.upper() for name in df.columns])
    return sf_df


def sf_upload_df(df, snow_session, **kwargs):
    sf_df = snow_session.session.create_dataframe([[float(val) if type(val) == np.float64 else val for val in row.values] for i, row in df.iterrows()],
                                                  schema=[name.upper() for name in df.columns])
    return sf_df


def get_current_exchange_dict(snow_session):
    return snow_session.tables.currency.to_pandas().set_index('CURRENCYUNIT')['EXCHANGERATE'].to_dict()


def col_renamer(colname:str) -> str:
    desc = {'POST' : 'POSTAGE',
            'LINEHAUL' : 'AWBLINEHAUL',
            'HAND' : 'HANDLING',
            'FUELSUR' : 'FUELSURCHARGE'}
    unit = {'PC' : '__PIECE',
            'LB' : '__LB',
            'KG' : '__KILO',
            'PERCENT' : '__PERCENT'}
    desc_part, unit_part = '', ''
    for from_desc, to_desc in desc.items():
        if from_desc in colname:
            desc_part = to_desc
            break
    for from_unit, to_unit in unit.items():
        if from_unit in colname:
            unit_part = to_unit
            break
    if not (desc_part or unit_part):
        print(f'no match found for {colname}, returning original')
        return colname
    return desc_part + unit_part


def prepare_temp_matrix_df(temp_matrix_path, office, vendorid, serviceid, vendor_override, currency_map:dict, acctnum = 0):
    matrix_df = (pd.read_excel(temp_matrix_path, keep_default_na=False)
                     .assign(office = office, vendorid = vendorid, serviceid = serviceid, 
                             vendor = vendor_override, acctnum = acctnum))
    matrix_df.columns = [name.upper() for name in matrix_df.columns]
    
    currency_to_cost_col = {
        'POSTCURRENCY': ['POSTPERPC', 'POSTPERKG','POSTPERLB'],
        'LHCURRENCY' : ['LINEHAULKG', 'LINEHAULLB'],
        'HANDCURRENCY': ['HANDPC', 'HANDKG', 'HANDLB'],
        'FUELSURCURRENCY' : ['FUELSURPERLB']
    }
    
    for currency_col, cols_to_convert in currency_to_cost_col.items():
        for col_to_convert in cols_to_convert:
            matrix_df[col_to_convert] = matrix_df[col_to_convert] * matrix_df[currency_col].map(lambda curr: 
                                                                                                currency_map.get(curr) 
                                                                                                if curr in currency_map
                                                                                                else 1)        
    rename_cols = ['POSTPERPC',
                 'POSTPERKG',
                 'POSTPERLB',
                 'LINEHAULKG',
                 'LINEHAULLB',
                 'HANDPC',
                 'HANDKG',
                 'HANDLB',
                 'FUELSURPERLB',
                 'FUELSURPERCENT']
    
    matrix_df = matrix_df.rename(columns={colname: col_renamer(colname) for colname in rename_cols})
    return matrix_df


def sf_get_pandas(sf_df:DataFrame, expected_cols:list) -> pd.DataFrame:
    collected_df = pd.DataFrame(sf_df.collect())
    return collected_df.assign(**{colname: None for colname in expected_cols}) if collected_df.empty else collected_df