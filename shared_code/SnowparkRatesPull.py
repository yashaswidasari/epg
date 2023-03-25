import asyncio
from shared_code.SnowparkSession import SnowflakeQuoterSession
from shared_code.SnowparkUtility import sf_upload_df
import datetime as dt
from snowflake.snowpark import DataFrame, Window
from snowflake.snowpark.functions import to_date, to_timestamp, lit, col, trim, round, when, coalesce
import snowflake.snowpark.functions as f
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class RatesPullFormats:
    base_rates: pd.DataFrame
    tariff: pd.DataFrame
    database: str

@dataclass
class IncreasesModel:
    quoteId: str
    service: int
    increase: float
    passthrough: bool
    margin: float = 0
    pickup: float = 0


def create_increases_col(df, increases, service_col):
    return 1 + df[service_col].map(lambda svc: increases.get(svc) if increases.get(svc) else 0)

def round_up(x:pd.Series, digits:int) -> float:
    rounded = (x*10**digits//1 + ((x*10**digits%1) >= 0.5))/100
    return rounded.round(digits)

ppnd_services = [30, 107, 108, 118]

## Utility and transformation steps ###################################################################################

def mirror_increases(increases:List[IncreasesModel])->List[IncreasesModel]:
    mirrors_map = {
        105: [117],
        106: [117],
        107:[118],
        108:[118],
        102:[119],
        45:[105, 106, 117],
        30:[107, 108, 118]
    }
    new_increases = increases.copy()
    #do jank stuff to preserve priority in mirrors map
    services = {increase['service']:increase for increase in increases}
    for svc_to_mirror, mirrors in mirrors_map.items():
        if svc_to_mirror in services:
            clone_params = services.get(svc_to_mirror).copy()
            clone_params.pop('service', None)
            mirror_rows = [dict(**clone_params, service = mirror) for mirror in mirrors]
            new_increases += mirror_rows
    return new_increases


def format_increases_df(increases:List[Dict]) -> pd.DataFrame:
    increases_df = (pd.DataFrame(increases)
                        .rename(columns={'service':'ORIGINAL_SERVICE', 'increase':'INCREASE_PCT'})
                        .drop_duplicates(['ORIGINAL_SERVICE'], keep='first'))
    return increases_df

## End Pandas Utility ##############

def backout_ppx_surcharges(session, rates, *args, **kwargs):
    """
    no default sch for the rate increase i'm not having it sift through the table for the one line everyone should have
    """
    surcharges = session.session.table('RATES_MANAGEMENT.INITIAL_PPX_SURCHARGES')
    
    base_rates = (rates.join(surcharges,
                           (rates['PRODUCT'] == surcharges['PRODUCT'])
                            & (rates['CTYCODE'] == surcharges['CTYCODE'])
                            & (rates['CUSTNO'] == surcharges['CUSTNO']),
                           'left')
                     .with_column('WT_RATE', rates['WT_RATE'] - coalesce(surcharges['WT_RATE'], lit(0)))
                     .select(*[rates[colname].alias(colname) for colname in rates.columns if colname != 'WT_RATE'],
                        col('WT_RATE')))
    
    undo_ca_surcharge = lambda rates_col: when((col('CTYCODE') == lit('CA')) 
                                                & (col('PRODUCT').isin(lit('01'), lit('05')))
                                                & (col('CUSTNO') != lit('6479')),
                                            col(rates_col) / lit(1.1)).otherwise(col(rates_col))
    
    return (base_rates
                .with_columns(['PC_RATE', 'WT_RATE'], [undo_ca_surcharge('PC_RATE'), undo_ca_surcharge('WT_RATE')]))


def remove_ppnd_overweight(session, rates, *args, **kwargs):
    is_ppnd = col('ORIGINAL_SERVICE').isin(*[lit(svc) for svc in ppnd_services])
    is_overweight = col('PC_WT_MAX') > lit(4.4)
    replace_5_lb = when(is_ppnd & (col('PC_WT_MAX') == 5) & (col('PC_WT_MIN') == 4), lit(4.4)).otherwise(col('PC_WT_MAX'))
    return rates.with_column('PC_WT_MAX', replace_5_lb).filter(~(is_ppnd & is_overweight))


def add_sell_zones(session, rates, *args, **kwargs):
    zones = session.session.table('RATES_MANAGEMENT.ZONES_MAP')
    rate_zones = rates.join(zones,
                           (rates['ORIGINAL_SERVICE'] == zones['XPO_SERVICE'])
                           & (rates['CTYCODE'] == zones['ORIGINAL_CTY']),
                           'left')
    
    return rate_zones.select(*[rates[colname].alias(colname) for colname in rates.columns],
                            zones['ZONE_CODE'])


def increase_zone_passthrough(session, rates_zones, *args, **kwargs):
    passthrough = session.session.table('RATES_MANAGEMENT.PASSTHROUGH_INCREASES')
    increases = rates_zones.join(passthrough,
                                 (passthrough['XPO_SERVICE'] == rates_zones['ORIGINAL_SERVICE'])
                                 & (passthrough['ZONE_CODE'] == rates_zones['ZONE_CODE'])
                                 & (passthrough['MIN_WT'] < rates_zones['PC_WT_MAX'])
                                 & (passthrough['MAX_WT'] >= rates_zones['PC_WT_MAX']))
    return increases.select(*[rates_zones[colname].alias(colname) for colname in rates_zones.columns if colname not in ('INCREASE_PCT', 'ZONE_CODE')],
                            passthrough['INCREASE_PCT'].alias('INCREASE_PCT'))


def apply_increase(session, rates_w_increases, rate_columns, start_date_column, *args, **kwargs):
    revised_cols = rate_columns + [start_date_column]
    revised_expressions = [round(col(rate_col)*(lit(1) + col('INCREASE_PCT')), 2) for rate_col in rate_columns] + [lit('1/23/2023')]
    return (rates_w_increases.with_columns([f'{colname}_OLD' for colname in rate_columns], [col(colname) for colname in rate_columns])
                .with_columns(revised_cols, revised_expressions))


def add_eps_sk(session, rates, epacket_increase_request, custno, *args, **kwargs):
    pickup = epacket_increase_request['pickup']
    margin = epacket_increase_request['margin']
    quotenum = epacket_increase_request['quoteId']
    equivalent_increase = 1/(1-margin) - 1
    pc_cost = 1.79 + .36
    wt_cost = .18 + .1 + pickup + 7.71 
    sk_row = {
        'ORIGINAL_SERVICE': 71,
        'CUSTNO': custno,
        'PRODUCT': '11',
        'SERVICE': 'EPS',
        'COUNTRY': 'SLOVAKIA',
        'CTYCODE': 'SK',
        'PC_WT_MIN': 0.0001,
        'PC_WT_MAX': 4.4,
        'WT_UNIT': 'LBS',
        'QUOTEID': quotenum,
        'INCREASE_PCT': equivalent_increase,
        'PC_RATE': pc_cost,
        'WT_RATE': wt_cost,
        'EFFECT_FR': '1/23/2023',
        'EFFECT_TO': '1/1/2050',
        'PASSTHROUGH': True
    }
    columns_not_present = {colname: lit(None) for colname in rates.columns if colname not in sk_row}
    sk_df = pd.DataFrame([sk_row])
    sk_sf = sf_upload_df(sk_df, session).with_columns(list(columns_not_present.keys()), list(columns_not_present.values()))
    return rates.union_all_by_name(sk_sf)

## End Utility and transformation steps ###################################################################################


async def get_increase_ppx_rates(custno, increases, eventloop, save_rates=False):
    """
    increases is dict by original service, 
    """
    session = SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')

    all_increases = mirror_increases(increases)
    increases_df = format_increases_df(all_increases)
    increase_services = increases_df.ORIGINAL_SERVICE.unique()
    increases_sf = sf_upload_df(increases_df, session)
    
    target_date = dt.datetime.today().strftime('%Y-%m-%d')
    date_filter = to_date(lit(target_date), 'yyyy-MM-dd').between(col('effect_fr'), col('effect_to'))
    ppx_rates = session.session.table('ODS.PPX_DBO_PARATE01')
    ppx_products = session.session.table('DATACLOUD.ODS.PPX_DBO_PRODUCT')
    ppx_tariff = (ppx_rates
                  .join(ppx_products, ppx_rates['PRODUCT'] == trim(ppx_products['PRODUCTCODE']), 'left')
                  .select(*[ppx_rates[col].alias(col) for col in ppx_rates.columns], 
                          ppx_products['XPOTRACKSERVICEID'].alias('ORIGINAL_SERVICE'))
                  .filter((col('CUSTNO') == lit(str(custno))) & date_filter))

    base_rates = backout_ppx_surcharges(session, ppx_tariff)
    if any([svc in increase_services for svc in ppnd_services]):
        base_rates = remove_ppnd_overweight(session, base_rates)
    base_rates_increases = base_rates.join(increases_sf, 'original_service')
    if not increases_df.query('passthrough').empty:
        passthrough_increases = base_rates_increases.filter(col('PASSTHROUGH'))
        steps = [add_sell_zones, increase_zone_passthrough]
        for step in steps:
            passthrough_increases = step(session, passthrough_increases)
            #eps_count = passthrough_increases.filter((col('ORIGINAL_SERVICE') == lit(71)) & (col('CTYCODE') != lit('CA'))).count()
        if 71 in increase_services:
            increase_71 = [increase for increase in increases if increase['service'] == 71][0]
            passthrough_increases = add_eps_sk(session, passthrough_increases, epacket_increase_request=increase_71, custno=custno)
        differences = [colname for colname in passthrough_increases.columns if colname not in base_rates_increases.columns]
        all_increases = (passthrough_increases
                            .union_all_by_name(base_rates_increases
                                                .with_columns(differences, [lit(None) for colname in differences])
                                                .filter(~col('PASSTHROUGH'))))
    else:
        all_increases = base_rates_increases

    increased_tariff = await eventloop.run_in_executor(
        None,
        lambda df: apply_increase(session, df, rate_columns=['PC_RATE', 'WT_RATE'], start_date_column='EFFECT_FR').cache_result(),
        all_increases
    )

    tariff_results = await eventloop.run_in_executor(
        None,
        lambda df: df.collect(),
        increased_tariff
    )
    updated_rates = pd.DataFrame(tariff_results)
    if updated_rates.empty:
        updated_rates = updated_rates.assign(**{colname: None for colname in ppx_tariff.columns})
    elif save_rates:
        await upload_rates_table(session=session, updated_sf=increased_tariff, target_table_name='RATES_MANAGEMENT.STAGE_PPX_PARATE01', eventloop=eventloop)

    """
    else:
        updated_rates = (rates_df
                         .assign(increase_pct = lambda df: create_increases_col(df, increases, 'ORIGINAL_SERVICE'))
                         .assign(PC_RATE = lambda df: round_up(df.PC_RATE.astype(float) * (df.increase_pct), 2),
                                 WT_RATE = lambda df: round_up(df.WT_RATE.astype(float) * (df.increase_pct), 2))
                         .drop(columns=['increase_pct']))
    """
    return RatesPullFormats(
        base_rates = updated_rates.rename(columns={'CTYCODE':'ORIGINAL_CTY'})
                        .assign(MAIL_TYPE = 'PR',
                                MAIL_FORMAT = 'PACK'),
        tariff = updated_rates,
        database = 'ppx')


async def get_increase_xpo_rates(custno, increases, eventloop, save_rates=False):
    """
    increases is dict by original service, 
    """
    session = SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')

    all_increases = mirror_increases(increases)
    increases_df = format_increases_df(all_increases)
    increases_sf = sf_upload_df(increases_df, session)
    
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
                    tblrates['SERVICEID'].alias('ORIGINAL_SERVICE'),
                    col('PRODUCTCODE').alias('PRODUCT'))
            .filter((col('ACCTNUM') == lit(str(custno))) & date_filter & placeholder_filter & (col('Active') == lit(1)))
        )

    updated_rates = await eventloop.run_in_executor(
        None,
        lambda df: apply_increase(session, df.join(increases_sf, 'ORIGINAL_SERVICE'), rate_columns=['PCCHARGE', 'WEIGHTCHARGE'], start_date_column='STARTDATE').cache_result(),
        xpo_rates
    )

    rates_async = updated_rates.collect_nowait()
    rates_results = await eventloop.run_in_executor(
        None,
        lambda df: df.result(),
        rates_async
    )
    rates_df = pd.DataFrame(rates_results)
    if rates_df.empty:
        rates_df = rates_df.assign(**{colname: None for colname in xpo_rates.columns})
    elif save_rates:
        await upload_rates_table(session=session, updated_sf=updated_rates, target_table_name='RATES_MANAGEMENT.STAGE_XPO_TBLRATES', eventloop=eventloop)

    return RatesPullFormats(
        base_rates = rates_df.rename(columns={'COUNTRYCODE':'ORIGINAL_CTY', 'PCCHARGE':'PC_RATE',
                                'WEIGHTCHARGE':'WT_RATE', 'MAILTYPE':'MAIL_TYPE', 'MAILFORMAT': 'MAIL_FORMAT'})
                        .assign(PC_WT_MIN = lambda df: df.MINOZ / 16, PC_WT_MAX = lambda df: df.MAXOZ / 16),
        tariff = rates_df,
        database = 'xpotrack'
        )

async def get_both_rates(custno, increases, eventloop, save_rates=False):
    return await asyncio.gather(get_increase_ppx_rates(custno, increases, eventloop, save_rates=save_rates), 
        get_increase_xpo_rates(custno, increases, eventloop, save_rates=save_rates))
    #return await asyncio.gather(get_increase_ppx_rates(custno, increases, eventloop))

async def upload_rates_table(session, updated_sf, target_table_name, eventloop):
    now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    common_cols = [colname 
                    for colname 
                    in session.session.table(target_table_name).columns
                    if colname not in ['CREATED_DATETIME', 'MODIFIED_DATETIME']]

    upload_sf = (updated_sf[common_cols]
                .with_columns(['CREATED_DATETIME', 'MODIFIED_DATETIME'], 
                              [to_timestamp(lit(now)), to_timestamp(lit(now))]))
    await eventloop.run_in_executor(
        None,
        lambda df: df.write.mode('append').save_as_table(target_table_name),
        upload_sf
    )


def get_lowest_rated_routes(snow_session: SnowflakeQuoterSession, wt_svcs: DataFrame,
                     mail_format = 'PACK', mail_type = 'PR', mode='PC'):
    """
    modes: PC - only use pc rate, PCLB - keep wt rate separate for pickup
    wt_svcs needs columns original_service, 
    """
    margin_factor = f.lit(1) - f.col('MARGIN')
    if mode.lower() == 'pc':
        pc_calc = (f.col('PC_RATE') + (f.col('WT_RATE') + f.col('PICKUP')) * f.col('PC_WT_MAX')) /margin_factor
        wt_calc = f.lit(0)
    else:
        pc_calc = f.col('PC_RATE') / margin_factor
        wt_calc = (f.col('WT_RATE') + f.col('PICKUP'))/margin_factor
    ranking_window = Window.partition_by(['WTSVC_ID', 'ORIGINAL_CTY']).order_by('PC_RATE')
                   
    rated_routes = snow_session.session.table('RATES_MANAGEMENT.RATED_ROUTES')
    retrieved_rates = (wt_svcs.join(rated_routes,
                                   (wt_svcs['WEIGHT'] > rated_routes['PC_WT_MIN'])
                                   & (wt_svcs['WEIGHT'] <= rated_routes['PC_WT_MAX'])
                                   & (wt_svcs['OFFICE'] == rated_routes['OFFICE'])
                                   & (wt_svcs['ORIGINAL_SERVICE'] == rated_routes['ORIGINAL_SERVICE'])
                                   & (rated_routes['MAIL_FORMAT'] == f.lit(mail_format)) & (rated_routes['MAIL_TYPE'] == f.lit(mail_type)),
                                   'inner')
                           .with_column('ranking', f.row_number().over(ranking_window))
                           .filter(f.col('ranking') == f.lit(1))
                           .with_columns(['PC_RATE', 'WT_RATE'], 
                                         [f.round(pc_calc, 2), f.round(wt_calc, 2)])
                           .select(
                                *[rated_routes[col].alias(col) for col in rated_routes.columns if col not in ['PC_RATE', 'WT_RATE']],
                               'QUOTEID', 'PICKUP', 'MARGIN', 'PC_RATE', 'WT_RATE'
                           ))
    return retrieved_rates