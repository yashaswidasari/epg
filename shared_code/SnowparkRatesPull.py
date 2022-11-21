import asyncio
from shared_code.SnowparkSession import SnowflakeQuoterSession
import datetime as dt
from snowflake.snowpark.functions import to_date, lit, col, trim, round
import pandas as pd
from dataclasses import dataclass

@dataclass
class RatesPullFormats:
    base_rates: pd.DataFrame
    tariff: pd.DataFrame
    database: str

@dataclass
class IncreasesModel:
    quote_id: str
    service: int
    increase: float


def create_increases_col(df, increases, service_col):
    return 1 + df[service_col].map(lambda svc: increases.get(svc) if increases.get(svc) else 0)

def round_up(x:pd.Series, digits:int) -> float:
    rounded = (x*10**digits//1 + ((x*10**digits%1) >= 0.5))/100
    return rounded.round(digits)


async def get_increase_ppx_rates(custno, increases, eventloop):
    """
    increases is dict by original service, 
    """
    session = SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')
    
    target_date = dt.datetime.today().strftime('%Y-%m-%d')
    date_filter = to_date(lit(target_date), 'yyyy-MM-dd').between(col('effect_fr'), col('effect_to'))
    ppx_rates = session.session.table('ODS.PPX_DBO_PARATE01')
    ppx_products = session.session.table('ODS.PPX_DBO_PRODUCT')
    ppx_tariff = (ppx_rates
                  .join(ppx_products, ppx_rates['PRODUCT'] == trim(ppx_products['PRODUCTCODE']), 'left')
                  .select(*[ppx_rates[col].alias(col) for col in ppx_rates.columns], 
                          ppx_products['XPOTRACKSERVICEID'].alias('ORIGINAL_SERVICE'))
                  .filter((col('CUSTNO') == lit(str(custno))) & date_filter))
    tariff_results = await eventloop.run_in_executor(
        None,
        lambda df: df.collect(),
        ppx_tariff
    )
    rates_df = pd.DataFrame(tariff_results)
    if rates_df.empty:
        updated_rates = rates_df.assign(**{colname: None for colname in ppx_tariff.columns})
    else:
        updated_rates = (rates_df
                         .assign(increase_pct = lambda df: create_increases_col(df, increases, 'ORIGINAL_SERVICE'))
                         .assign(PC_RATE = lambda df: round_up(df.PC_RATE.astype(float) * (df.increase_pct), 2),
                                 WT_RATE = lambda df: round_up(df.WT_RATE.astype(float) * (df.increase_pct), 2))
                         .drop(columns=['increase_pct']))
    return RatesPullFormats(
        base_rates = updated_rates.rename(columns={'CTYCODE':'ORIGINAL_CTY'})
                        .assign(MAIL_TYPE = 'PR',
                                MAIL_FORMAT = 'PACK'),
        tariff = updated_rates,
        database = 'ppx')


async def get_increase_xpo_rates(custno, increases, eventloop):
    """
    increases is dict by original service, 
    """
    session = SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')
    
    target_date = dt.datetime.today().strftime('%Y-%m-%d')
    date_filter = to_date(lit(target_date), 'yyyy-MM-dd').between(col('StartDate'), col('EndDate'))
    placeholder_filter = (col('PCCHARGE') > lit(0)) | (col('WEIGHTCHARGE') > lit(0))
    tblrates = session.session.table('ODS.XPO_DBO_TBLRATES')
    ppx_products = session.session.table('ODS.PPX_DBO_PRODUCT')
    xpo_rates = (
        tblrates
            .join(ppx_products, 
            ppx_products['XPOTRACKSERVICEID'] == tblrates['SERVICEID'],
            'left')
            .with_columns(['MINOZ', 'MAXOZ'], [round(col('MINOZ'), 3), round(col('MAXOZ'), 3)])
            .select(*[tblrates[col].alias(col) for col in tblrates.columns],
                    col('PRODUCTCODE').alias('PRODUCT'))
            .filter((col('ACCTNUM') == lit(str(custno))) & date_filter & placeholder_filter & (col('Active') == lit(1)))
        )
    rates_async = xpo_rates.collect_nowait()
    rates_results = await eventloop.run_in_executor(
        None,
        lambda df: df.result(),
        rates_async
    )
    rates_df = pd.DataFrame(rates_results)
    if rates_df.empty:
        updated_rates = rates_df.assign(**{colname: None for colname in xpo_rates.columns})
    else:
        updated_rates = (
            rates_df
                .assign(increase_pct = lambda df: create_increases_col(df, increases, 'SERVICEID'))
                .assign(PCCHARGE = lambda df: round_up(df.PCCHARGE.astype(float) * ( df.increase_pct), 2),
                        WEIGHTCHARGE = lambda df: round_up(df.WEIGHTCHARGE.astype(float) * (df.increase_pct), 2))
                .drop(columns=['increase_pct'])
        )

    return RatesPullFormats(
        base_rates = updated_rates.rename(columns={'COUNTRYCODE':'ORIGINAL_CTY', 'SERVICEID':'ORIGINAL_SERVICE', 'PCCHARGE':'PC_RATE',
                                'WEIGHTCHARGE':'WT_RATE', 'MAILTYPE':'MAIL_TYPE', 'MAILFORMAT': 'MAIL_FORMAT'})
                        .assign(PC_WT_MIN = lambda df: df.MINOZ / 16, PC_WT_MAX = lambda df: df.MAXOZ / 16),
        tariff = updated_rates,
        database = 'xpotrack'
        )

async def get_both_rates(custno, increases, eventloop):
    return await asyncio.gather(get_increase_ppx_rates(custno, increases, eventloop), get_increase_xpo_rates(custno, increases, eventloop))