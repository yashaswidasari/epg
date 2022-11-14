from shared_code.ExcelFiller.FillerInput import snowflake_quote_to_filler_input
from shared_code.SnowparkStartGrids import create_multi_wt_svc, MultiWeightServiceModel
from shared_code.SnowparkSession import SnowflakeQuoterSession
from shared_code.SnowparkRatesPull import get_increase_ppx_rates, get_increase_xpo_rates
from shared_code.SalesforceRateResponseModels import format_pc_lb
from shared_code.ExcelFiller.RateTemplateManager import ShoppedTemplateManager, PcLbZoneManager, GCZoneManager
from shared_code.DriveComms.DriveComms import DriveComms
from shared_code.SnowparkGridTransforms import (except_final_services, match_matrix_rows, 
    filter_matrix_prefers, matrix_pivot_details, quote_matrix_details_pc, get_lowest_cost_pc, quote_matrix_details_lb)
from shared_code.ExcelFiller.FillerInput import (FillerInputGenerator, ServiceMapFromExcel, BaseRatesFromPandas, 
                                                 ZoneMapFromExcel, SurchargesDummy, QuoteParamsModel)
from collections import defaultdict
from io import BytesIO
import base64
import pandas as pd

def get_quote_filler_generator(request):
    quote_params = QuoteParamsModel(cust_name = request['custName'], 
                                    quote_num = request['quoteNum'], 
                                    quote_date = request['quoteDate'])
    service_map = ServiceMapFromExcel('mock_tables/service_map.xlsx')
    #not sure if different ones needed for new quotes but here are default zones
    default_zones = ZoneMapFromExcel('mock_tables/zone_maps.xlsx')
    
    weight_sets = {
        'oz': [i/16 for i in range(1, 71)] + [4.4],
        'lb': [0.5] + [float(i) for i in range(1, 67)],
        'gc_lb': [float(i) for i in range(1, 151)],
        'packetmaxwt': [4.4]
    }

    shop_weights = {
        102 : 'oz',
        105: 'lb',
        106: 'lb',
        107: 'oz',
        108: 'oz',
        33: 'gc_lb',
        71: 'packetmaxwt'
    }

    eps_weights = {
        71: 'packetmaxwt'
    }
    
    requested_services = [int(svc) for svc in request['services'].split(',')]
    
    weight_grids = defaultdict(list)
    for svc in requested_services:
        weight_grids[shop_weights[svc]].append(svc)
        
    session = SnowflakeQuoterSession(configs_path='snowflake_config.json', mode='configs')
    
    wt_svcs = [MultiWeightServiceModel(weight_sets[set_name], services)
                  for set_name, services
                  in weight_grids.items()]
    
    grid = create_multi_wt_svc(session, wt_svcs=wt_svcs, location=request['location'], custno='0', mail_format='PACK', mail_type='PR')
    
    steps = [except_final_services, match_matrix_rows, filter_matrix_prefers, 
             matrix_pivot_details, quote_matrix_details_pc, get_lowest_cost_pc]

    for step in steps:
        grid = step(session, grid, margin=request['margin'], pickup=request['pickup'])
        
    results = pd.DataFrame(grid.collect())
    results_filler = snowflake_quote_to_filler_input(results)
    
    filler_input_generator = FillerInputGenerator(base_rates = BaseRatesFromPandas(results_filler), 
                                                  service_map= service_map, 
                                                  zone_mapper= default_zones, 
                                                  surcharges= SurchargesDummy(), 
                                                  quote_params= quote_params)
    
    return filler_input_generator


class FillerPasser:
    def __init__(self):
        self.template_map = {
            'ePG Parcel': ShoppedTemplateManager('excel_templates/parceltariff.xlsx'),
            'ePacket': PcLbZoneManager('excel_templates/eps.xlsx')
        }
        self.service_mapper = {code: 'ePG Parcel' for code in [102, 105, 106, 107, 108]}
        self.service_mapper[71] = 'ePacket'
        
    def pass_fillers(self, fillers, quote_params, svc_id_dict=None):
        svc_id_dict = {} if not svc_id_dict else svc_id_dict
        organized_fillers = defaultdict(list)
        response = []
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        for filler in fillers:
            if filler.service_id in self.service_mapper:
                organized_fillers[self.service_mapper[filler.service_id]].append(filler)
        for template_name, template_fillers in organized_fillers.items():
            safe_cust_name = quote_params.cust_name.replace('/', '').replace(':', '')
            filename = f'{template_name} {safe_cust_name} ({quote_params.quote_num}).xlsx'
            template = self.template_map[template_name]
            io = BytesIO()
            wb = template.save_new_workbook(filename, template_fillers, quote_params, in_mem=True)
            wb.save(io)
            #ugh need to test anything
            response.append({
                'type': template_name,
                'filename': filename,
                'content': base64.b64encode(io.getvalue()).decode(),
                'services': [{
                    'service': template_filler.service_id,
                    'quoteId':svc_id_dict.get(template_filler.service_id)} for template_filler in template_fillers],
                'mimetype': mimetype
            })
        return response


def save_quote_sheet(request):
    quote_params = QuoteParamsModel(cust_name = request['custName'], 
                                    quote_num = request['quoteNum'], 
                                    quote_date = request['quoteDate'])
    fill_logic = FillerPasser()
    fillers = get_quote_filler_generator(request).split_rates_by_svc()
    return fill_logic.pass_fillers(fillers, quote_params)


def try_parse_int(x):
    try:
        return int(x)
    except:
        return x


def save_increase(request, drive_comms:DriveComms):
    quote_params = QuoteParamsModel(cust_name = request['custName'], 
                                    quote_num = request['quoteNum'], 
                                    quote_date = request['quoteDate'])
    custno = request['custno']
    fill_logic = FillerPasser()
    increases = {try_parse_int(increase['service']) : increase['increase'] for increase in request['increases']}
    svc_id_dict = {try_parse_int(increase['service']) : increase['quoteId'] for increase in request['increases']}
    updated_rates = get_increase_ppx_rates(custno, increases)
    base_rates = updated_rates.base_rates
    service_map = ServiceMapFromExcel('mock_tables/service_map.xlsx')
    #replace somewhere
    default_zones = ZoneMapFromExcel('mock_tables/zone_maps.xlsx')
    generator = FillerInputGenerator(base_rates = BaseRatesFromPandas(base_rates), 
                                                  service_map= service_map, 
                                                  zone_mapper= default_zones, 
                                                  surcharges= SurchargesDummy(), 
                                                  quote_params= quote_params)
    fillers = generator.split_rates_by_svc()
    response = {}
    rate_cards = fill_logic.pass_fillers(fillers, quote_params, svc_id_dict)
    if rate_cards:
        response['rateCards'] = rate_cards

    updated_xpo = get_increase_xpo_rates(custno, increases)
    base_xpo = updated_xpo.base_rates
    if not base_xpo.empty:
        response['pcLbRates'] = [
            {
                'service': service,
                'quoteId': svc_id_dict.get(service),
                'rates': format_pc_lb(rates)
            }
            for service, rates in base_xpo.groupby('ORIGINAL_SERVICE')]

    tariff = updated_rates.tariff
    if not tariff.empty:
        io = BytesIO()
        tariff.to_csv(io, index=False)
        tariff_filename = f'{custno}_ppx({quote_params.quote_num}).csv'
        drive_comms.save_file_bytes(io.getvalue(), tariff_filename, f'ppx/{quote_params.quote_num}')
    xpo_uploads = updated_xpo.tariff
    if not xpo_uploads.empty:
        xpo_io = BytesIO()
        xpo_uploads.to_csv(xpo_io, index=False)
        tariff_filename = f'{custno}_xpo({quote_params.quote_num}).csv'
        drive_comms.save_file_bytes(xpo_io.getvalue(), tariff_filename, f'xpo/{quote_params.quote_num}')

    return response


def confirm_quotes(quote_num, custno, services, drive_comms:DriveComms):
    blob_container = 'testthingy'
    new_files = []
    for folder in ['xpo', 'ppx']:
        upload_files = drive_comms.list_files(blob_container, f'{folder}/{quote_num}')
        for file in upload_files:
            new_path = f"{folder}_ready/{file.split('/')[-1]}"
            new_file = drive_comms.copy_file(blob_container, file, blob_container, new_path)
            new_files.append(new_file)
    return new_files