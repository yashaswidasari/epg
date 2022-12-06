import logging
import asyncio
import azure.functions as func
from shared_code.AppWrappers import save_increase
import json
from shared_code.DriveComms.DriveComms import AzureDriveComms, LocalDriveComms

#drive_comms = LocalDriveComms('C:/Users/rtse/Documents/Python Scripts/epg wkg/2022-08-10 Rate Card Flask Demo/dummy_drive')
#drive_comms = AzureDriveComms('azure_storage_config.json')

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    required_fields = ['increases', 'custno', 'custName', 'quoteNum', 'quoteDate', 'saveRates']
    #increases is dict of svc:pct
    request = {field: req_body.get(field) for field in required_fields}
    print(request)

    eventloop = asyncio.get_event_loop()

    response = await save_increase(request, eventloop)

    return func.HttpResponse(json.dumps(response))
