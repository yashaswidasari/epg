import logging

import azure.functions as func
from shared_code.AppWrappers import save_increase
import json
from shared_code.DriveComms.DriveComms import AzureDriveComms, LocalDriveComms

#drive_comms = LocalDriveComms('C:/Users/rtse/Documents/Python Scripts/epg wkg/2022-08-10 Rate Card Flask Demo/dummy_drive')
drive_comms = AzureDriveComms('DefaultEndpointsProtocol=https;AccountName=trialbucket;AccountKey=nso4v25EyPNC1DlW/UxwBeN1WI29/xtLv1IMcaN2vHqrothZt9CoQ5OyV4roZ4x88aEI3M+7ZV6QYSzZN1lzrw==;EndpointSuffix=core.windows.net')

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    required_fields = ['increases', 'custno', 'custName', 'quoteNum', 'quoteDate']
    #increases is dict of svc:pct
    request = {field: req_body.get(field) for field in required_fields}
    print(request)

    response = save_increase(request, drive_comms)

    return func.HttpResponse(json.dumps(response))
