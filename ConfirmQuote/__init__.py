import logging

import azure.functions as func
from dataclasses import dataclass
import json
from shared_code.DriveComms.DriveComms import AzureDriveComms
from shared_code.AppWrappers import confirm_quotes
from typing import List

drive_comms = AzureDriveComms('DefaultEndpointsProtocol=https;AccountName=trialbucket;AccountKey=nso4v25EyPNC1DlW/UxwBeN1WI29/xtLv1IMcaN2vHqrothZt9CoQ5OyV4roZ4x88aEI3M+7ZV6QYSzZN1lzrw==;EndpointSuffix=core.windows.net')

@dataclass
class ConfirmQuoteRequest:
    quoteNum: str
    custno: str
    services: List[str]

def main(req: func.HttpRequest) -> func.HttpResponse:

    req_body = req.get_json()
    confirm_params = ConfirmQuoteRequest(**req_body)
    new_files = confirm_quotes(confirm_params.quoteNum, confirm_params.custno, confirm_params.services, drive_comms=drive_comms) 

    response = {'new_files':new_files}

    #new_path = drive_comms.copy_file()
    return func.HttpResponse(json.dumps(response))
