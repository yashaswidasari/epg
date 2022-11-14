import logging

import azure.functions as func
from shared_code.AppWrappers import save_quote_sheet
import json


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    required_fields = ['services', 'location', 'pickup', 'margin', 'cust_name', 'quote_num', 'quote_date']
    request = {field: req_body.get(field) for field in required_fields}

    response = save_quote_sheet(request)

    return func.HttpResponse(json.dumps(response))