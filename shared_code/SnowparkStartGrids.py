# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 21:35:55 2022

Each grid intended to rate out separately using a unique PieceID

@author: rtse
"""

from snowflake.snowpark import Session, Window, DataFrame
from snowflake.snowpark.functions import (col, lit, row_number, coalesce, when, sum, min, to_date, current_timestamp, 
                                          max, concat_ws, round, replace, upper, regexp_count, left, substr, length)
import datetime as dt
import json
import os
from dataclasses import dataclass
from typing import List

from shared_code.SnowparkSession import SnowflakeQuoterSession
        
        
#Getting Volume/Weight Breaks to rate out##############################################################################       

def get_volume_edw(snow_session, filter_expression):
    parcels = snow_session.session.table('edw.parcel')
    products = snow_session.session.table('edw.product')
    countries = snow_session.session.table('edw.country')
    offices = snow_session.session.table('edw.facility')
    customers = snow_session.session.table('edw.customer').with_column_renamed('COUNTRY_CODE', 'CUST_COUNTRY_CODE')
    jobs = snow_session.session.table('edw.job')
    job_services = snow_session.session.table('edw.job_service')

    routing_search = when(coalesce(col('ROUTING_TYPE_ID'), lit(1)) == lit(2), col('XPO_TRACK_SERVICEID')).otherwise(col('SERVICE_CODE'))
    office_fix = when(col('FACILITY_CODE') == lit('LAX'), lit('XPO')).otherwise(col('FACILITY_CODE'))

    volume = (parcels
                  .join(countries, 'COUNTRY_ID', 'left')
                  .join(offices, 'FACILITY_ID', 'left')
                  .join(customers, 'CUSTOMER_ID', 'left')
                  .join(products, 'PRODUCT_ID', 'left')
                  .join(jobs, 'JOB_ID', 'left')
                  .join(job_services, 'JOB_SERVICE_ID', 'left')
                  .select(
                          parcels['PARCEL_ID'].alias('piece_id'),
                          parcels['CUSTNO'].alias('custno'),
                          customers['CLIENT_NAME'],
                          parcels['FACILITY_ID'],
                          countries['COUNTRY_CODE'].alias('original_cty'),
                          countries['COUNTRY_CODE'].alias('ctycode'),
                          routing_search.alias('original_service'),
                          office_fix.alias('office'),
                          col('XPO_MAIL_FORMAT').alias('mail_format'),
                          col('XPO_MAIL_TYPE').alias('mail_type'),
                          lit(1).alias('pieces'),
                          col('WEIGHT'),
                          col('DIMENSION_LENGTH').alias('dim_l'),
                          col('DIMENSION_WIDTH').alias('dim_w'),
                          col('DIMENSION_HEIGHT').alias('dim_h'),
                          col('DATE_PROCESSED'),
                          parcels['ZIP'],
                          parcels['ADDRESS1'].alias('ADDRESS1'), 
                          parcels['ADDRESS2'].alias('ADDRESS2'), 
                          parcels['ADDRESS3'].alias('ADDRESS3'),
                          parcels['REFNO'],
                          parcels['ROUTENM'],
                          parcels['POSTAGE'].alias('RECORDED_COST'),
                          parcels['DUTY'].alias('DUTY'),
                          parcels['TAX'].alias('TAX'),
                          parcels['PACKAGE_VALUE'].alias('PACKAGE_VALUE'),
                          lit(0).alias("IS_APT"),
                          lit(0).alias("IS_BOX")
                        )
                  .filter(filter_expression)
                  .na.fill('', subset=['ADDRESS1', 'ADDRESS2', 'ADDRESS3']))
    
    return volume


def get_volume_tblpresortclosed(snow_session, filter_expression):
    mail = snow_session.session.table('ODS.XPO_DBO_TBLPRESORTCLOSED')
    parcel01 = snow_session.session.table('ODS.PPX_DBO_PARCEL01')
    pacust01 = snow_session.session.table('ODS.PPX_DBO_PACUST01')
    product = snow_session.session.table('ODS.PPX_DBO_PRODUCT')
    customers = snow_session.session.table('ODS.XPO_DBO_TBLCLIENTS')
    jobs = snow_session.session.table('ODS.XPO_DBO_TBLJOB')

    routing_search = when(coalesce(pacust01['ROUTINGTYPEID'], lit(1)) == lit(2), product['XPOTrackServiceId']).otherwise(jobs['Service'])

    volume = (mail
                  .join(jobs, 'Jobnumber', 'left')
                  .join(customers, 'AcctNum', 'left')
                  .join(pacust01, pacust01['CUSTNO'] == customers['AcctNum'], 'left')
                  .join(parcel01, parcel01['XPOPIECEID'] == mail['PIECEID'], 'left')
                  .join(product, product['PRODUCTCODE'] == parcel01['PROD_CODE'], 'left')
                  .select(
                         mail['PIECEID'].alias('piece_id'),
                         jobs['AcctNum'].alias('custno'),
                         customers['CLIENTNAME'],
                         mail['COUNTRYCODE'].alias('original_cty'),
                         mail['COUNTRYCODE'].alias('ctycode'),
                         routing_search.alias('original_service'),
                         mail['LOCATION'].alias('office'),
                         left(mail['MailFormat'], 2).alias('mail_type'),
                         substr(mail['MailFormat'], 4, length(mail['MailFormat']) - lit(3)).alias('mail_format'),
                         mail['PIECES'],
                         mail['WEIGHT'],
                         coalesce(col('DIM_L'), lit(0)).alias('dim_l'),
                         coalesce(col('DIM_W'), lit(0)).alias('dim_w'),
                         coalesce(col('DIM_H'), lit(0)).alias('dim_h'),
                         col('ENTEREDDATE'),
                         parcel01['ZIP'],
                         parcel01['ADDRESS1'].alias('ADDRESS1'), 
                         parcel01['ADDRESS2'].alias('ADDRESS2'), 
                         parcel01['ADDRESS3'].alias('ADDRESS3'),
                         parcel01['REFNO'],
                         mail['MATRIXID'].alias('RECORDED_MATRIXID'),
                         mail['COST'].alias('RECORDED_COST_XPO'),
                         lit(0).alias("IS_APT"),
                         lit(0).alias("IS_BOX")
                        )
                  .filter(filter_expression)
                  .na.fill('', subset=['ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'ZIP']))
    
    return volume


def get_active_rates(snow_session: SnowflakeQuoterSession, custno, prod_codes=None):
    parate01 = snow_session.tables.parate01
    rates_filter = col('custno') == lit(custno)
    if prod_codes:
        if type(prod_codes) == str:
            rates_filter = rates_filter & (col('product') == lit(prod_codes))
    return (
        parate01.filter( rates_filter
                        & (current_timestamp().between(col('effect_fr'), col('effect_to'))))
            .select('ctycode', 'product', 'pc_wt_min', 'pc_wt_max', 'pc_rate', 'wt_rate')
        )
        
               
def create_svc_grid(snow_session: SnowflakeQuoterSession, weights: list,
                    service = '3', location = 'XPO', custno = '0', 
                    mail_format = 'FLAT', mail_type = 'PB'):
    session = snow_session.session
    countries = snow_session.tables.countries
    weight_breaks = session.create_dataframe([weight for weight in weights]).to_df("weight")
    id_window = Window.order_by(['ctycode', 'weight'])
    
    return (
        countries.join(weight_breaks, None, 'full')
            .select(
                     row_number().over(id_window).alias('piece_id'),
                     lit(custno).alias('custno'),
                     col('ctycode').alias('original_cty'),
                     col('ctycode'),
                     lit(service).alias('original_service'),
                     lit(location).alias('office'),
                     lit(mail_format).alias('mail_format'),
                     lit(mail_type).alias('mail_type'),
                     lit(1).alias('pieces'),
                     col('weight'),
                     lit(0).alias('dim_l'),
                     lit(0).alias('dim_w'),
                     lit(0).alias('dim_h'),
                     lit(0).alias('is_apt'),
                     lit(0).alias('is_box')
                    )
            )


def create_multi_grid(snow_session: SnowflakeQuoterSession, weights: list,
                    services: list = ['3'], location = 'XPO', custno = '0', 
                    mail_format = 'FLAT', mail_type = 'PB'):
    """
    doesn't consider different sets of weights currently
    """
    session = snow_session.session
    countries = snow_session.tables.countries
    curr_services = session.create_dataframe([svc for svc in services]).to_df('curr_services')
    weight_breaks = session.create_dataframe([weight for weight in weights]).to_df("weight")
    ppx_products = session.table('ODS.PPX_DBO_PRODUCT')
    id_window = Window.order_by(['ctycode', 'weight'])
    
    return (
        countries.join(weight_breaks, None, 'full')
            .join(curr_services, None, 'full')
            .join(ppx_products, 
                  ppx_products['XPOTRACKSERVICEID'] == curr_services['curr_services'],
                  'left')
            .select(
                     row_number().over(id_window).alias('piece_id'),
                     lit(custno).alias('custno'),
                     col('ctycode').alias('original_cty'),
                     col('ctycode'),
                     col('curr_services').alias('original_service'),
                     col('PRODUCTCODE').alias('PRODUCT'),
                     lit(location).alias('office'),
                     lit(mail_format).alias('mail_format'),
                     lit(mail_type).alias('mail_type'),
                     lit(1).alias('pieces'),
                     col('weight'),
                     lit(0).alias('dim_l'),
                     lit(0).alias('dim_w'),
                     lit(0).alias('dim_h'),
                     lit(0).alias('is_apt'),
                     lit(0).alias('is_box')
                    )
            )

@dataclass
class MultiWeightServiceModel:
    weights: List[float]
    services: List[int]

def create_multi_wt_svc(snow_session: SnowflakeQuoterSession, wt_svcs: List[MultiWeightServiceModel],
                        location = 'XPO', custno = '0', mail_format = 'FLAT', mail_type = 'PB'):
    """
    doesn't consider different sets of weights currently
    """
    session = snow_session.session
    countries = snow_session.tables.countries
    services = wt_svcs[0].services
    weights = wt_svcs[0].weights
    curr_services = session.create_dataframe([svc for svc in services]).to_df('curr_services')
    weight_breaks = session.create_dataframe([weight for weight in weights]).to_df("weight")
    ppx_products = session.table('ODS.PPX_DBO_PRODUCT')
    
    grid = (countries.join(weight_breaks, None, 'full')
            .join(curr_services, None, 'full')
            .join(ppx_products, 
                  ppx_products['XPOTRACKSERVICEID'] == curr_services['curr_services'],
                  'left')
            .select(
                     lit(custno).alias('custno'),
                     col('ctycode').alias('original_cty'),
                     col('ctycode'),
                     col('curr_services').alias('original_service'),
                     col('PRODUCTCODE').alias('PRODUCT'),
                     col('weight')
                    ))
    if len(wt_svcs) > 1:
        for wt_svc in wt_svcs[1:]:
            services = wt_svc.services
            weights = wt_svc.weights
            curr_services = session.create_dataframe([svc for svc in services]).to_df('curr_services')
            weight_breaks = session.create_dataframe([weight for weight in weights]).to_df("weight")

            grid_next = (countries.join(weight_breaks, None, 'full')
                            .join(curr_services, None, 'full')
                            .join(ppx_products, 
                                  ppx_products['XPOTRACKSERVICEID'] == curr_services['curr_services'],
                                  'left')
                            .select(
                                     lit(custno).alias('custno'),
                                     col('ctycode').alias('original_cty'),
                                     col('ctycode'),
                                     col('curr_services').alias('original_service'),
                                     col('PRODUCTCODE').alias('PRODUCT'),
                                     col('weight')
                                    ))
            grid = grid.union_all_by_name(grid_next)
            
    id_window = Window.order_by(['ctycode', 'original_service', 'weight'])

    return (
            grid.select(
                     row_number().over(id_window).alias('piece_id'),
                     *[grid[col].alias(col) for col in grid.columns],
                     lit(location).alias('office'),
                     lit(mail_format).alias('mail_format'),
                     lit(mail_type).alias('mail_type'),
                     lit(1).alias('pieces'),
                     lit(0).alias('dim_l'),
                     lit(0).alias('dim_w'),
                     lit(0).alias('dim_h'),
                     lit(0).alias('is_apt'),
                     lit(0).alias('is_box')
                    )
            )