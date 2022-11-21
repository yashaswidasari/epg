# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 14:19:26 2022

@author: rtse
"""

from shared_code.ExcelFiller.RateTabFiller import RateTabFiller, WeightBreakFiller, PcLbZoneFiller, WeightBreakZoneFiller
from shared_code.ExcelFiller.FillerInput import FillerInputModel, QuoteParamsModel
import openpyxl as xl
import pandas as pd
from io import BytesIO
from dataclasses import dataclass
from typing import List, Dict, Tuple
from abc import abstractmethod, ABC


# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 14:19:26 2022

@author: rtse
"""


import openpyxl as xl
import pandas as pd
from io import BytesIO
from dataclasses import dataclass
from typing import Dict, List, Set


class RateCardManager(ABC):
    
    def __init__(self, template_path: str, template_source_tab: str = None,
                 start_tabs: List[str] = None, end_tabs: List[str] = None, *args, **kwargs):
        self.template_path = template_path
        self.template_source_tab = template_source_tab
        with open(self.template_path, 'rb') as f:
            self.template = BytesIO(f.read())
        self.start_tabs = start_tabs
        self.end_tabs = end_tabs
            
    def save_new_workbook(self, filename: str, filler_inputs: List[FillerInputModel], 
                          quote_params: QuoteParamsModel, in_mem: bool = True):
        wb = xl.load_workbook(self.template)
        all_tabs_used = []
        for filler_input in filler_inputs:
            target_tab, tab_filler = self.assign_filler_logic(filler_input)
            tab_filler.fill_tab(wb, filler_input, target_tab=target_tab)
            all_tabs_used.append(target_tab)
        self.cleanup_workbook(wb, all_tabs_used, filler_inputs, quote_params)
        if in_mem:
            return wb
        wb.save(filename)
        
    def cleanup_workbook(self, wb, all_tabs_used, filler_inputs, *args, **kwargs):
        all_tabs_keep = self.check_keep_tabs(all_tabs_used, filler_inputs)
        self.delete_unused_tabs(wb, all_tabs_keep)
        self.reorder_tabs(wb, all_tabs_keep)
        
    def delete_unused_tabs(self, wb: xl.workbook.workbook.Workbook, tabs_used: List[str]):
        for sheet in wb.sheetnames:
            if sheet not in tabs_used:
                del wb[sheet]
                
    def reorder_tabs(self, wb, tabs_used):
        current_index = [wb.sheetnames.index(tab) for tab in tabs_used]
        wb._sheets = [wb._sheets[i] for i in current_index]
                
    @abstractmethod
    def assign_filler_logic(self, filler_input: FillerInputModel) -> Tuple[str, RateTabFiller]:
        pass
    
    @abstractmethod
    def check_keep_tabs(self, all_tabs_used:List[str], filler_inputs:List[FillerInputModel]):
        pass
    
    
class ShoppedTemplateManager(RateCardManager):
    
    cover_name_cell = 'B2'
    cover_quotenum_cell = 'B3'
    cover_date_cell = 'B4'
    cover_source_tab = 'Cover Page'
    generic_shopped_sheet = WeightBreakFiller(rate_start_row=9, rate_start_col=4, 
                                              source_tab='Shopped Lb', name_width=15, name_row=5,
                                              name_space=1)
    
    def assign_filler_logic(self, filler_input):
        return filler_input.service_abbr, self.generic_shopped_sheet
    
    def check_keep_tabs(self, all_tabs_used:List[str], filler_inputs:List[FillerInputModel]):
        return [self.cover_source_tab] + all_tabs_used + ['Disclosure', 'CA FSC']
    
    def cleanup_workbook(self, wb, all_tabs_used, filler_inputs, quote_params: QuoteParamsModel, *args, **kwargs):
        self.fill_cover_page(wb, quote_params)
        super().cleanup_workbook(wb, all_tabs_used, filler_inputs)
        
    def fill_cover_page(self, wb, quote_params):
        wb[self.cover_source_tab][self.cover_name_cell].value = quote_params.cust_name
        wb[self.cover_source_tab][self.cover_quotenum_cell].value = quote_params.quote_num
        wb[self.cover_source_tab][self.cover_date_cell].value = quote_params.quote_date
        
        
class PcLbZoneManager(RateCardManager):
    
    def assign_filler_logic(self, filler_input: FillerInputModel) -> Tuple[str, RateTabFiller]:
        return self.template_source_tab, PcLbZoneFiller(rate_start_row=11, rate_start_col=4,
                                    name_cells = ['B7'], source_tab= self.template_source_tab,
                                    zones_to_int=True)
    
    def check_keep_tabs(self, all_tabs_used:List[str], filler_inputs:List[FillerInputModel]):
        add_end_tabs = self.end_tabs if self.end_tabs else []
        return all_tabs_used + add_end_tabs


class PcLbEPSManager(PcLbZoneManager):
    
    def assign_filler_logic(self, filler_input: FillerInputModel) -> Tuple[str, RateTabFiller]:
        return self.template_source_tab, PcLbZoneFiller(rate_start_row=11, rate_start_col=4,
                                    name_cells = ['B7'], source_tab= self.template_source_tab,
                                    zones_to_int=True)

class PcLbIPAManager(PcLbZoneManager):
    
    def assign_filler_logic(self, filler_input: FillerInputModel) -> Tuple[str, RateTabFiller]:
        return self.template_source_tab, PcLbZoneFiller(rate_start_row=12, rate_start_col=4,
                                    name_cells = ['B7'], source_tab= self.template_source_tab,
                                    zones_to_int=True)
    
    
class WtBreakZoneManager(RateCardManager):
    
    def assign_filler_logic(self, filler_input: FillerInputModel) -> Tuple[str, RateTabFiller]:
        return self.template_source_tab, WeightBreakZoneFiller(rate_start_row=13, rate_start_col=2,
                                              name_cells = ['A8'], source_tab=self.template_source_tab,
                                              name_row=8)
    
    def check_keep_tabs(self, all_tabs_used:List[str], filler_inputs:List[FillerInputModel]):
        add_end_tabs = self.end_tabs if self.end_tabs else []
        return all_tabs_used + add_end_tabs