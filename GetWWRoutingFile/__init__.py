import logging

import azure.functions as func

import datetime as dt
import pandas as pd
import json
import shared_code.SnowparkGridTransforms as srp
import shared_code.SnowparkWWTransforms as ww
from shared_code.SnowparkSession import SnowflakeQuoterSession


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    request_parsed = {key: req.form[key] for key in ['service', 'custno', 'location', 'mail_type']}

    session = srp.SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')
    ww_file = req.files['wwfile']
    ww_file_stream = ww_file.stream
    ww_file_stream.seek(0)
    ww_file_content = ww_file_stream.read()

    """
    req_json = req.get_json()
    request_parsed = {key: req_json[key] for key in ['service', 'custno', 'location', 'mail_type']}

    session = srp.SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')
    ww_file_content = req_json['wwfile']
    """
    ww_table = ww.import_inmem_ww(session, ww_file_content).cache_result()

    grid = ww.create_ww_grid(session, step_size = 1/64, upper_limit = 1.1, **request_parsed)

    pipeline = [srp.except_final_services, srp.match_matrix_rows, 
                srp.filter_matrix_prefers, srp.matrix_pivot_details,
                srp.quote_matrix_details_pc, srp.get_lowest_cost_pc, 
                ww.format_ww_intervals, ww.format_ww_file]

    results = grid
    for step in pipeline:
        results = step(session, results, ww_table = ww_table)
    print('Snowflake query initialized, pulling data...')

    results_df = results.to_pandas()
    print('Pull Complete')
    results_file = results_df.to_csv(path_or_buf=None, header = None, sep='\t', index=False)
    headers = {
      "Access-Control-Allow-Origin": "*"
    }
    return func.HttpResponse(results_file, headers=headers)