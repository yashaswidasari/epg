from snowflake.snowpark import Session, Window, DataFrame
from snowflake.snowpark.functions import (col, lit, row_number, coalesce, when, sum, min, to_date, current_timestamp, 
                                          max, concat_ws, round, replace, upper, regexp_count, concat)
import datetime as dt

from shared_code.SnowparkUtility import encode_dense_zip_col
from shared_code.SnowparkSession import SnowflakeQuoterSession


def check_apt_box(snow_session, volume, address_rules, *args, **kwargs):
    address_match = (volume.join(address_rules, None, 'full')
                         .with_column('EXPR_SEARCH', 
                                      regexp_count(volume['ADDRESS1'], address_rules['VALUE']) 
                                          + regexp_count(volume['ADDRESS2'], address_rules['VALUE'])
                                          + regexp_count(volume['ADDRESS3'], address_rules['VALUE']))
                         .with_column('EXPR_TYPE', when(col('FLAGTYPEID') == lit(2), 'IS_APT').otherwise('IS_BOX')) #hunt down flag types
                         .select(*[volume[name].alias(name) for name in volume.columns],
                                col('EXPR_TYPE'),
                                col('EXPR_SEARCH'))
                         .pivot(col('EXPR_TYPE'), ['IS_APT', 'IS_BOX'])
                         .sum(col('EXPR_SEARCH'))
                         .select(*[volume[colname] for colname in volume.columns if colname not in ['IS_APT', 'IS_BOX']],
                                 col("'IS_APT'").alias('IS_APT'),
                                 col("'IS_BOX'").alias('IS_BOX')))
    return address_match


def add_post_zones_lh(snow_session, post_volume: DataFrame, current_zones_override=None, *args, **kwargs):
    current_zones = current_zones_override if current_zones_override else snow_session.session.table('ODS.XPO_DBO_CANADAZONES')
    ei_linehaul = snow_session.session.table('ODS.XPO_DBO_ENTRYINDUCTIONLINEHAUL')
    ei_points = snow_session.session.table('ODS.XPO_DBO_ENTRYINDUCTIONPOINTS')
    cust_induct = snow_session.session.table('ODS.XPO_DBO_CUSTOMERINDUCTION').with_column_renamed('CUSTNO', 'CUST_INDUCT_CUSTNO')
    
    custspecific_inducted = post_volume.join(cust_induct,
                                  (cust_induct['CUST_INDUCT_CUSTNO'] == post_volume['custno'])
                                  & (post_volume['office'] == cust_induct['location'])
                                  & (post_volume['original_cty'] == cust_induct['countrycode']))
    
    default_inducted = post_volume.join(cust_induct,
                                        (cust_induct['CUST_INDUCT_CUSTNO'] == '0')
                                        & (post_volume['office'] == cust_induct['location'])
                                        & (post_volume['original_cty'] == cust_induct['countrycode']))
    
    default_filtered = default_inducted.join(custspecific_inducted,
                                             (default_inducted['CUSTNO'] == custspecific_inducted['CUST_INDUCT_CUSTNO'])
                                                 & (default_inducted['LOCATION'] == custspecific_inducted['LOCATION'])
                                                 & (default_inducted['INDENTRYID'] == custspecific_inducted['INDENTRYID']),
                                             'left_anti')
    
    all_inducted = (custspecific_inducted.union_all(default_filtered)
                        .filter(encode_dense_zip_col(col('zip'), 3).between(encode_dense_zip_col(col('PostCodeStart'), 3), 
                                                                            encode_dense_zip_col(col('PostCodeEnd'), 3))))
    
    volume_inducted = (all_inducted
                           .join(ei_points, 'IndEntryId')
                           .join(ei_linehaul, 
                                 (ei_linehaul['InductionId'] == ei_points['InductionId'])
                                 & (ei_linehaul['EntryId'] == ei_points['Entry'])
                                 & (ei_linehaul['custno'] == lit(0)))
                           .select(*[all_inducted[name].alias(name) for name in post_volume.columns],
                                   cust_induct['ID'].alias('CUST_INDUCT_ID'),
                                   cust_induct['CUST_INDUCT_CUSTNO'],
                                   ei_linehaul['PerLb'],
                                   ei_linehaul['InductionId'].alias('INDUCTIONID'),
                                   ei_points['INDENTNAME']))
    
    zone_join = (volume_inducted.join(current_zones,
                                  encode_dense_zip_col(volume_inducted['zip'], 6).between(encode_dense_zip_col(current_zones['StartPost'], 6), 
                                                                                                encode_dense_zip_col(current_zones['EndPost'], 6))
                                  & (volume_inducted['INDUCTIONID'] == current_zones['INDUCTIONID']))
                .join(snow_session.tables.vendors.select(col('VENDORID'), col('CANADAZONEPREFIX')), 
                      'VENDORID')
                .with_column('CTYCODE', concat(coalesce(col('CANADAZONEPREFIX'), lit('')), col('ZONE')))
                .with_column_renamed('PERLB', 'EILH_LB'))
    
    additional_cols = ['EILH_LB', 'INDENTNAME']
    
    return (zone_join.select(*[zone_join[colname] for colname in post_volume.columns],
                             *[zone_join[colname] for colname in additional_cols])
                .union_all_by_name(post_volume.with_column('EILH_LB', lit(0))
                                       .with_column('INDENTNAME', lit('XX'))))


def except_final_services(snow_session:SnowflakeQuoterSession, routing_grid, **kwargs):
    exceptions = snow_session.tables.exceptions
    all_services = snow_session.tables.all_services
    except_window = Window.partition_by(col('piece_id')).order_by([col('custno').desc(), col('CountryCode').desc(), col('Exceptionid')])

    except_routes = (
        routing_grid
            .join(exceptions,
                 ( ((routing_grid['ctycode'] == exceptions['CountryCode']) | (exceptions['CountryCode'] == lit('ZZ')))
                                 & (exceptions['FromService'] == routing_grid['original_service'])
                                 & (exceptions['AcctNum'] == routing_grid['custno']) ),
                 'left')
            .with_column('except_priority', row_number().over(except_window))
            .with_column('parent_service', coalesce(col('ToService'), col('original_service')))
            .filter(col('except_priority') == lit(1))
            .select(exceptions['ToService'], 
                    exceptions['Exceptionid'],
                    col('parent_service'),
                    *[routing_grid[col].alias(col) for col in routing_grid.columns])
        )

    return (
        except_routes
            .join(all_services,
                  'parent_service')
            .select(all_services['ServiceID'].alias('routing_service'),
                    *[except_routes[col].alias(col) for col in except_routes.columns])
        )


def match_matrix_rows(snow_session: SnowflakeQuoterSession, volume, vendor_override=None, **kwargs):
    vendors = vendor_override if vendor_override else snow_session.tables.vendors
    target_date = dt.datetime.today().strftime('%Y-%m-%d')
    matrix = snow_session.tables.matrix.filter(to_date(lit(target_date), 'yyyy-MM-dd').between(col('StartDate'), col('EndDate')))
    matrix_augmented = (matrix.join(vendors.select('VendorID', 'Vendor', 
                                                   when(coalesce(col('DimWeight'), lit(0)) == 1, 
                                                        lit(1) / col('DimDivisor')).otherwise(lit(0)).alias('DimFactor'),
                                                   'POBox', 'Suite', 'AllowDDU', 'AllowDDP', 'LengthMax', 'WidthMax',
                                                   'HeightMax', 'LengthGirthAddMax', 'LWHAddMax', 'LWHMultiplyMax'),
                                    'VendorID', 
                                    'left'))
    
    vendor_dim_wt = (volume['DIM_L'] * volume['DIM_W'] * volume['DIM_H'] * matrix_augmented['DimFactor'])
    vendor_bill_wt = when(volume['Weight'] < vendor_dim_wt, vendor_dim_wt).otherwise(volume['Weight'])
    matrix_kg_selector = vendor_bill_wt/2.2046/volume['pieces']-0.0001
    
    dim_max_selector = ((volume['DIM_L'] <= matrix_augmented['LengthMax'])
                        & (volume['DIM_W'] <= matrix_augmented['WidthMax'])
                        & (volume['DIM_H'] <= matrix_augmented['HeightMax']))
    lg_max_selector = (volume['DIM_L'] + 2 * (volume['DIM_W'] + volume['DIM_H'])) <= matrix_augmented['LengthGirthAddMax']
    lwhadd_max_selector = (volume['DIM_L'] + volume['DIM_W'] + volume['DIM_H']) <= matrix_augmented['LWHAddMax']
    lwhmult_max_selector = (volume['DIM_L'] * volume['DIM_W'] * volume['DIM_H']) <= matrix_augmented['LWHMultiplyMax']
    dims_selector = dim_max_selector & lg_max_selector & lwhadd_max_selector & lwhmult_max_selector
    
    weight_selector = (matrix_kg_selector > matrix_augmented['MinKg']) & (matrix_kg_selector <= matrix_augmented['MaxKg'])
    format_selector = ((matrix_augmented['MailFormat'] == volume['mail_format']) | (matrix_augmented['MailFormat'] == lit('ALL')))
    type_selector = (matrix_augmented['MailType'] == volume['mail_type']) | ((matrix_augmented['MailType'] == lit('PM')) & (volume['mail_type'] == lit('PB')))
    service_selector = matrix_augmented['SERVICEID'] == volume['ROUTING_SERVICE']
    pobox_filter = when(~matrix_augmented['POBOX'], volume["IS_BOX"] == lit(0)).otherwise(lit(True))
    apt_filter = when(~matrix_augmented['SUITE'], volume["IS_APT"] == lit(0)).otherwise(lit(True))
    acct_selector = (volume['custno'] == matrix_augmented['AcctNum']) | (matrix_augmented['AcctNum'] == lit('0'))
    office_selector = volume['OFFICE'] == matrix_augmented['LOCATION']
    cty_selector = matrix_augmented['COUNTRYCODE'] == volume['CTYCODE']
    
    total_filter = (office_selector & cty_selector & acct_selector
                     & format_selector & service_selector
                     & weight_selector & type_selector
                     & dims_selector & pobox_filter & apt_filter)
    
    #mtx_audit_cols = ['MinKg', 'MaxKg', 'DimFactor']
    mtx_audit_cols = []
    return (
        volume
            .join(matrix_augmented,
                  total_filter)
            .select(*[volume[colname].alias(colname) for colname in volume.columns],
                    vendor_bill_wt.alias('vendor_bill_wt'),
                    matrix_augmented['MatrixID'],
                    matrix_augmented['KicksIn'],
                    matrix_augmented['AcctNum'].alias('mAcctNum'),
                    matrix_augmented['Vendor'],
                    matrix_augmented['VendorID'],
                    *[matrix_augmented[colname].alias(colname) 
                      if colname not in volume.columns 
                      else matrix_augmented[colname].alias(f'm{colname}')
                      for colname in mtx_audit_cols])
    )


def filter_matrix_prefers(snow_session: SnowflakeQuoterSession, matrix_grid, **kwargs):
    nonprefers = snow_session.tables.nonprefers
    prefers = snow_session.tables.prefers
    matrix_rows_nonpref_selection = (matrix_grid.join(nonprefers,
                                              ( ((nonprefers['CountryCode'] == matrix_grid['ORIGINAL_CTY']) | (nonprefers['CountryCode'] == lit('ZZ')))
                                               & ((nonprefers['AcctNum'] == matrix_grid['custno']) | (nonprefers['AcctNum'] == lit('0')))
                                               & (nonprefers['ServiceID'] == matrix_grid['parent_service'])
                                               & (nonprefers['VendorID'] == matrix_grid['VendorID']) ),
                                              'left_anti'))

    matrix_rows_pref_selection = (matrix_rows_nonpref_selection
                                      .join(prefers,
                                        ( ((prefers['CountryCode'] == matrix_rows_nonpref_selection['ORIGINAL_CTY']) | (prefers['CountryCode'] == lit('ZZ')))
                                               & ((prefers['AcctNum'] == matrix_rows_nonpref_selection['custno']) | (prefers['AcctNum'] == lit('0')))
                                               & (prefers['ServiceID'] == matrix_rows_nonpref_selection['parent_service'])
                                               & (prefers['VendorID'] == matrix_rows_nonpref_selection['VendorID']) ),
                                        'left')
                                      .select(prefers['PreferredID'],
                                              *[matrix_rows_nonpref_selection[col].alias(col) for col in matrix_rows_nonpref_selection.columns])
                                 )
    
    return matrix_rows_pref_selection


mtx_identity_cols = ['piece_id', 'MatrixID', 'ctycode', 'mAcctNum', 'Weight', 
                     'WeightKicksInDiff', 'MinKg', 'MaxKg', 'original_service', 'routing_service', 
                     'VendorID', 'Vendor', 'mail_format']

def matrix_pivot_details(snow_session: SnowflakeQuoterSession, matrix_grid, pivot_corrections:dict=None, **kwargs):
    """
    pivot corrections is dict of colname to correct, col expression
    """
    rating_cols = ['POSTAGE__PIECE',
     'HANDLING__PIECE',
     'POSTAGE__KILO',
     'FUELSURCHARGE__PERCENT',
     'POSTAGE__LB',
     'HANDLING__LB', 'AWBLINEHAUL__LB', 'AWBLINEHAUL__KILO']
    
    
    matrix_det = snow_session.tables.matrix_det
    currency = snow_session.tables.currency
    units = snow_session.tables.units
    ratetype = snow_session.tables.ratetype

    matrix_id_window = Window.partition_by(col('MatrixID'))

    exchange_hotfix = when(col('CostDesc') == lit('FUELSURCHARGE__PERCENT'), lit(1)).otherwise(col('ExchangeRate'))
    
    matrix_det_join = (matrix_grid
                        .join(matrix_det, 'MatrixID')
                        .join(currency, 'CurrencyUnit')
                        .join(units, 'Unit')
                        .join(ratetype, 'RateType'))
    if pivot_corrections:
        matrix_det_join = matrix_det_join.select(*[expr.alias(colname) 
                                                       for colname, expr in pivot_corrections.items()],
                                                 *[matrix_det_join[colname].alias(colname) 
                                                       for colname in matrix_det_join.columns 
                                                       if colname not in pivot_corrections])
        #print(matrix_det_join.columns) will need to watch out for casing not sure best place to do it

    matrix_costs = (matrix_det_join
                        .withColumn('CostDesc', upper(concat_ws(lit('__'), replace(col('RTDESCRIPTION'), ' ', ''), col('UNITABBR'))))
                        .withColumn('AdjRate', col('Rate') * exchange_hotfix)
                        .select(col('AdjRate'), col('CostDesc'), *[matrix_grid[col].alias(col) for col in matrix_grid.columns])
                        .pivot(col('CostDesc'), rating_cols)
                        .sum(col('AdjRate'))
                        .na.fill(0, subset=[f"'{col}'" for col in rating_cols]))
    return matrix_costs.select(*[matrix_costs[colname].alias(colname.replace("'", "")) for colname in matrix_costs.columns])


def match_temp_matrix_rows(snow_session, volume, temp_matrix, vendor_override=None, *args, **kwargs):
    vendors = vendor_override if vendor_override else snow_session.tables.vendors
    
    temp_mtx_augmented = temp_matrix.join(vendors.select('VendorID',
                                                   when(coalesce(col('DimWeight'), lit(0)) == 1, 
                                                        lit(1) / col('DimDivisor')).otherwise(lit(0)).alias('DimFactor'),
                                                   'POBox', 'Suite', 'AllowDDU', 'AllowDDP', 'LengthMax', 'WidthMax',
                                                   'HeightMax', 'LengthGirthAddMax', 'LWHAddMax', 'LWHMultiplyMax'),

                                    'VendorID', 
                                    'left')
    
    vendor_dim_wt = (volume['DIM_L'] * volume['DIM_W'] * volume['DIM_H'] * temp_mtx_augmented['DimFactor'])
    vendor_bill_wt = when(volume['Weight'] < vendor_dim_wt, vendor_dim_wt).otherwise(volume['Weight'])
    matrix_kg_selector = vendor_bill_wt/2.2046/volume['pieces']-0.0001
    
    dim_max_selector = ((volume['DIM_L'] <= temp_mtx_augmented['LengthMax'])
                        & (volume['DIM_W'] <= temp_mtx_augmented['WidthMax'])
                        & (volume['DIM_H'] <= temp_mtx_augmented['HeightMax']))
    lg_max_selector = (volume['DIM_L'] + 2 * (volume['DIM_W'] + volume['DIM_H'])) <= temp_mtx_augmented['LengthGirthAddMax']
    lwhadd_max_selector = (volume['DIM_L'] + volume['DIM_W'] + volume['DIM_H']) <= temp_mtx_augmented['LWHAddMax']
    lwhmult_max_selector = (volume['DIM_L'] * volume['DIM_W'] * volume['DIM_H']) <= temp_mtx_augmented['LWHMultiplyMax']
    dims_selector = dim_max_selector & lg_max_selector & lwhadd_max_selector & lwhmult_max_selector

    weight_selector = (matrix_kg_selector > temp_mtx_augmented['MinKg']) & (matrix_kg_selector <= temp_mtx_augmented['MaxKg'])
    format_selector = ((temp_mtx_augmented['MailFormat'] == volume['mail_format']) | (temp_mtx_augmented['MailFormat'] == lit('ALL')))
    type_selector = (temp_mtx_augmented['MailType'] == volume['mail_type']) | ((temp_mtx_augmented['MailType'] == lit('PM')) & (volume['mail_type'] == lit('PB')))
    service_selector = (temp_mtx_augmented['SERVICEID'] == volume['ROUTING_SERVICE']) | (temp_mtx_augmented['SERVICEID'] == lit(-1))
    pobox_filter = when(~temp_mtx_augmented['POBOX'], volume["IS_BOX"] == lit(0)).otherwise(lit(True))
    apt_filter = when(~temp_mtx_augmented['SUITE'], volume["IS_APT"] == lit(0)).otherwise(lit(True))
    office_selector = volume['OFFICE'] == temp_mtx_augmented['OFFICE']
    cty_selector = temp_mtx_augmented['COUNTRYCODE'] == volume['CTYCODE']
    acct_selector = (volume['custno'] == temp_mtx_augmented['AcctNum']) | (temp_mtx_augmented['AcctNum'] == lit(0))
    
    rating_cols = ['POSTAGE__PIECE',
     'HANDLING__PIECE',
     'POSTAGE__KILO',
     'FUELSURCHARGE__PERCENT',
     'POSTAGE__LB',
     'HANDLING__LB', 'AWBLINEHAUL__LB', 'AWBLINEHAUL__KILO']

    cost_matched_pre = (volume
                           .join(temp_mtx_augmented,
                                 office_selector & cty_selector
                                     & format_selector & service_selector
                                     & weight_selector & type_selector
                                     & pobox_filter & apt_filter & dims_selector & acct_selector)
                            .select(*[volume[name].alias(name) for name in volume.columns],
                                    vendor_bill_wt.alias('vendor_bill_wt'),
                                    temp_mtx_augmented['MatrixID'],
                                    temp_mtx_augmented['KicksIn'],
                                    temp_mtx_augmented['AcctNum'].alias('mAcctNum'),
                                    lit(1).alias('is_temp_mtx'),
                                    temp_mtx_augmented['Vendor'],
                                    temp_mtx_augmented['VendorID'],
                                    *[temp_mtx_augmented[colname] for colname in rating_cols]))
    
    return cost_matched_pre


def quote_matrix_details_pc(snow_session: SnowflakeQuoterSession, matrix_grid, margin:float=0, pickup:float=0, force_pack_labor=False, **kwargs):
    weight_kicks_in = when((col('KicksIn') * lit(2.2046) * col('pieces')) < col('vendor_bill_wt'), 
                                                   col('vendor_bill_wt') - col('KicksIn') * lit(2.2046) * col('pieces')).otherwise(lit(0))
    
    post_calc = (col('POSTAGE__PIECE') * col('PIECES')
                + (col('POSTAGE__LB') + col('POSTAGE__KILO') / 2.2046) * weight_kicks_in) * (1 + col('FUELSURCHARGE__PERCENT'))
    if force_pack_labor:
        labor_calc = when(col('vendor_bill_wt') < lit(10), lit(.34) + lit(.17) * col('WEIGHT')).otherwise(lit(2))
        labor_dim_calc = when(col('vendor_bill_wt') < lit(10), lit(.34) + lit(.17) * col('vendor_bill_wt')).otherwise(lit(2))
    else:
        labor_calc = col('HANDLING__PIECE') * col('PIECES') + col('HANDLING__LB') * col('WEIGHT')
        labor_dim_calc = col('HANDLING__PIECE') * col('PIECES') + col('HANDLING__LB') * col('vendor_bill_wt')
    lh_calc = (col('AWBLINEHAUL__LB') + col('AWBLINEHAUL__Kilo')/lit(2.2046)) * col('WEIGHT')
    lh_dim_calc = (col('AWBLINEHAUL__LB') + col('AWBLINEHAUL__Kilo')/lit(2.2046)) * col('vendor_bill_wt')
    if 'EILH_LB' in matrix_grid.columns:
        lh_calc = lh_calc + col('EILH_LB') * col('WEIGHT')
        lh_dim_calc = lh_dim_calc + col('EILH_LB') * col('vendor_bill_wt')
    pu_calc = lit(pickup) * col('WEIGHT')
    
    if margin > 0:
        pc_rate_expr = round((post_calc + labor_calc + lh_calc + pu_calc) / lit(1 - margin), 2)
        pc_rate_dim_expr = round((post_calc + labor_dim_calc + lh_dim_calc + pu_calc) / lit(1 - margin), 2)
    else:
        pc_rate_expr = post_calc + labor_calc + lh_calc + pu_calc
        pc_rate_dim_expr = post_calc + labor_dim_calc + lh_dim_calc + pu_calc
        

    grid_tariff = (matrix_grid
                       .select(*[matrix_grid[colname].alias(colname) for colname in matrix_grid.columns],
                               post_calc.alias('total_postage'),
                               labor_calc.alias('total_labor'),
                               lh_calc.alias('total_linehaul'),
                               pc_rate_expr.alias('pc_rate'),
                               lit(0).alias('wt_rate'),
                               pc_rate_dim_expr.alias('pc_rate_dimall'))
                       .sort([col('ctycode'), col('Weight'), col('pc_rate')]))
    return grid_tariff


def get_lowest_cost_pc(snow_session: SnowflakeQuoterSession, cost_grid, **kwargs):
    added_columns = [when(col('mAcctNum') != lit(0), lit(1)).otherwise(lit(0)).alias('is_loaded')]
    if 'PREFERREDID' in cost_grid.columns:
        added_columns.append(when(col('PreferredID').isNotNull(), lit(1)).otherwise(lit(0)).alias('is_preferred'))         
        ranking_priority = [col('is_loaded').desc(), col('is_preferred').desc(), col('pc_rate_dimall')]
    else:
        ranking_priority = [col('is_loaded').desc(), col('pc_rate_dimall')]
        
    ranking_window = (Window.partition_by(col('piece_id'))
                      .order_by(ranking_priority))
    grid_tariff = (cost_grid
                       .select(*[cost_grid[colname].alias(colname) for colname in cost_grid.columns],
                               *added_columns)
                       .with_column('cost_rank', row_number().over(ranking_window))
                       .filter(col('cost_rank') == 1))
    
    return grid_tariff