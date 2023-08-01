import numpy as np
import pandas as pd
from snowflake.snowpark import Window, DataFrame
from snowflake.snowpark.functions import col, lit, row_number, coalesce, when, sum, min, to_date, max, concat_ws, round, lag, lpad
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

from shared_code.SnowparkSession import SnowflakeQuoterSession


def create_ww_grid(snow_session: SnowflakeQuoterSession, step_size = 1/32, upper_limit = 1.1, 
                    service = '3', location = 'XPO', custno = '0', mail_type = 'PM'):
    
    idmail_cty = (snow_session.session.table('EDW.COUNTRY')
                      .filter(col('IDMAILGROUPCODE').is_not_null())
                      .select(col('COUNTRY_CODE').alias('ctycode'),
                              col('IDMAILGROUPCODE')))
    idmail_formats = snow_session.session.table('REFERENCE.WW_FORMATS')
    
    test_weights = [float(wt) for wt in np.arange(step_size, upper_limit, step_size)]
    weight_breaks = snow_session.session.create_dataframe([weight for weight in test_weights]).to_df("weight")

    id_window = Window.order_by(['mail_format', 'ctycode', 'weight'])

    grid = (
        idmail_cty.join(weight_breaks, None, 'full')
            .join(idmail_formats, None, 'full')
            .filter(col('weight') <= col('MaxLb'))
            .select(
                    row_number().over(id_window).alias('piece_id'),
                    lit(custno).alias('custno'),
                    col('ctycode').alias('original_cty'),
                    col('ctycode'),
                    lit(service).alias('original_service'),
                    lit(location).alias('office'),
                    col('mail_format'),
                    lit(mail_type).alias('mail_type'),
                    lit(1).alias('pieces'),
                    col('weight'),
                    lit(0).alias('dim_l'),
                    lit(0).alias('dim_w'),
                    lit(0).alias('dim_h'),
                    lit(0).alias("IS_APT"),
                    lit(0).alias("IS_BOX"),
                    col('maxwidth'),
                    col('maxheight'),
                    col('maxthickness'),
                    col('idmailgroupcode')
                    )
            )
    return grid


def import_local_ww(snow_session, ww_filename):
    """
    Note this returns both the ww_table to be used later in format_ww_file along with the weight grid used to start the pipeline.
    step_size indicates resolution and endpoints of the weight grid, try to keep fractions of half ounces at the moment to accomodate limits
    """
    ww_file = pd.read_excel(ww_filename, keep_default_na=False, dtype=str).reset_index()
    
    ww_schema = StructType([
        StructField("ww_index", IntegerType()),
        StructField("Bin", StringType()), 
        StructField("Country", StringType()),
        StructField("Vendor", StringType()),
        StructField("Format", StringType())
        ])
    
    #expanding as list avoids having to rename columns that are getting quotes
    ww_table = snow_session.session.create_dataframe([list(row.values) for i, row in ww_file.iterrows()], schema=ww_schema)
    
    return ww_table


def import_local_ww_file(snow_session, ww_filename, step_size = 1/32):
    """
    Note this returns both the ww_table to be used later in format_ww_file along with the weight grid used to start the pipeline.
    step_size indicates resolution and endpoints of the weight grid, try to keep fractions of half ounces at the moment to accomodate limits
    """
    ww_file = pd.read_excel(ww_filename, sheet_name='Bins', keep_default_na=False)
    routing_options = pd.read_excel(ww_filename, sheet_name='RoutingOptions', dtype=str).loc[0].to_dict()
    temp_file = ww_filename.replace('.xlsx', '.csv')
    ww_file.to_csv(temp_file, header=False)
    
    sf_stage = '@~'
    sf_stage_folder = 'staged'
    snow_session.session.file.put(temp_file,  f'{sf_stage}/{sf_stage_folder}', overwrite=True, auto_compress=False)
    
    ww_schema = StructType([
        StructField("ww_index", IntegerType()),
        StructField("Bin", StringType()), 
        StructField("Country", StringType()),
        StructField("Vendor", StringType()),
        StructField("Format", StringType())
        ])
    
    ww_table = (snow_session.session
                 .read
                 .option("SKIP_HEADER", 0)
                 .schema(ww_schema)
                 .csv(f'{sf_stage}/{sf_stage_folder}/{temp_file}'))
    
    ww_grid = create_ww_grid(snow_session, step_size=step_size, **routing_options)
    
    return ww_table, ww_grid


def import_inmem_ww(snow_session, ww_bytes):
    ww_file = pd.read_excel(ww_bytes, keep_default_na=False, dtype=str).reset_index()
    
    ww_schema = StructType([
        StructField("ww_index", IntegerType()),
        StructField("Bin", StringType()), 
        StructField("Country", StringType()),
        StructField("Vendor", StringType()),
        StructField("Format", StringType())
        ])
    
    ww_table = snow_session.session.create_dataframe([list(row.values) for i, row in ww_file.iterrows()], schema=ww_schema)
    
    return ww_table

def format_ww_intervals(snow_session: SnowflakeQuoterSession, cost_grid, **kwargs):
    sort_cols = ['MAIL_FORMAT', 'CTYCODE', 'WEIGHT']
    change_window = Window.order_by(sort_cols)
    group_change_condition = (((lag(col('Vendor')).over(change_window) != col('Vendor')) 
                                   | (lag(col('ctycode')).over(change_window) != col('ctycode')))
                              & (lag(col('Vendor')).over(change_window).is_not_null()))

    group_interval_add_window = Window.order_by(sort_cols).range_between(Window.unboundedPreceding, 0)
    group_window = Window.partition_by(col('group_number')).order_by(sort_cols)
    group_max_window = Window.partition_by(col('group_number'))
    #lag(lead?) the vendor column, weight column
    weight_lag_window = Window.partition_by(['ctycode', 'mail_format']).order_by('weight')
    short_results = (cost_grid
                         .with_column('prev_weight', lag(col('weight')).over(weight_lag_window))
                         .na.fill(0, subset=['prev_weight'])
                         .with_column('group_change', when(group_change_condition, 1).otherwise(0))
                         .with_column('group_number', sum(col('group_change')).over(group_interval_add_window))
                         .with_column('group_order', row_number().over(group_window))
                         .with_column('group_wt_max', max(col('WEIGHT')).over(group_max_window))
                         .filter(col('group_order') == 1))
    return short_results


def format_ww_file(snow_session: SnowflakeQuoterSession, cost_intervals:DataFrame, ww_table:DataFrame=None, **kwargs):
    final_select_cols = ['IDMailGroupCode', 'prev_weight', 'group_wt_max', 'IndiciaNumber', 'Bin', 
                     'MaxBagWeight', 'MinWidth', 'MaxWidth', 'MinHeight', 'MaxHeight', 
                     'MinThickness', 'MaxThickness', 'Zone', 'Rate', 'Bagging', 'Print', 'CodeToPrint']
    
    idmail_indicias = snow_session.session.table('REFERENCE.WW_INDICIAS')
    ww_file_rank_window = Window.partition_by(col('piece_id')).order_by(col('ww_index'), idmail_indicias['ctycode'])

    file_joined = (cost_intervals.join(ww_table, 
                                (
                                    ((ww_table['COUNTRY'] == cost_intervals['CTYCODE']) | (ww_table['COUNTRY'] == lit('ZZ')))
                                    & ((ww_table['VENDOR'] == cost_intervals['VENDOR']) | (ww_table['VENDOR'] == lit('ALL')))
                                    & ((ww_table['FORMAT'] == cost_intervals['MAIL_FORMAT']) | (ww_table['FORMAT'] == lit('ALL')))
                                ),
                                'left')
                       .join(idmail_indicias, 
                                (
                                    ((idmail_indicias['ctycode'] == cost_intervals['CTYCODE']) | (idmail_indicias['ctycode'] == lit('ZZ')))
                                    & (idmail_indicias['VENDOR'] == cost_intervals['VENDOR'])
                                    & ((idmail_indicias['FORMAT'] == cost_intervals['MAIL_FORMAT']) | (idmail_indicias['FORMAT'] == lit('ALL')))
                                ),
                                'left')
                       .with_column('ww_rank', row_number().over(ww_file_rank_window))
                       .filter(col('ww_rank') == lit(1))
                       .with_column('FormattedBin', coalesce(lpad(col('BIN'), 3, lit('0')), lit('000')))
                       .with_column('CheckedIndicia', coalesce(col('IndiciaNumber'), lit('00')))
                       .select(*[cost_intervals[col].alias(col) 
                                 for col in final_select_cols 
                                 if col.lower() in [res_col.lower() for res_col in cost_intervals.columns]],
                              col('FormattedBin').alias('Bin'),
                              col('CheckedIndicia').alias('IndiciaNumber'),
                              lit(99999.999999).alias('MaxBagWeight'),
                              lit(0).alias('MinWidth'),
                              lit(0).alias('MinHeight'),
                              lit(0).alias('MinThickness'),
                              col('FormattedBin').alias('Zone'),
                              lit(1).alias('Rate'),
                              lit('Y').alias('Bagging'),
                              lit('Y').alias('Print'),
                              col('FormattedBin').alias('CodeToPrint'))
                       .sort([col('IDMailGroupCode'), col('MinWidth'), col('MaxWidth'), col('MinHeight'), col('MaxHeight'), col('MinThickness'), col('MaxThickness'), col('prev_weight'), col('group_wt_max'), col('MaxBagWeight')]))
    
    return file_joined.select(final_select_cols)