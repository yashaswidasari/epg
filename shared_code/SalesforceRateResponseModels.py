from dataclasses import dataclass
from typing import List
import pandas as pd

@dataclass
class PcLbRate:
    Country__c: str
    Mail_Format__c: str
    Mail_Type__c: str
    Max_Oz__c: float
    Min_Oz__c: float
    Min_Pieces__c: int
    Per_Lb__c: float
    Per_Pc__c: float

class PcLbRates:
    rates: List

    def __init__(self, base_rates: pd.DataFrame):
        """
        base_rates should be in the common format
        """
        renames = {
            'ORIGINAL_CTY': 'Country__c',
            'MAIL_FORMAT': 'Mail_Format__c',
            'MAIL_TYPE': 'Mail_Type__c',
            'WT_RATE': 'Per_Lb__c',
            'PC_RATE': 'Per_Pc__c',
        }

        transforms = {
            'Max_Oz__c': lambda df: df['PC_WT_MAX'] * 16,
            'Min_Oz__c': lambda df: df['PC_WT_MIN'] * 16,
            'Min_Pieces__c': lambda df: 0,
        }

        rates_df = (
            base_rates
                .rename(columns = renames)
                .assign(**transforms)
                [list(renames.keys()) + list(transforms.key())]
        )

        self.rates = rates_df.to_dict(orient='records')


def format_pc_lb(base_rates: pd.DataFrame):
    """
    base_rates should be in the common format
    """
    renames = {
        'ORIGINAL_CTY': 'Country__c',
        'MAIL_FORMAT': 'Mail_Format__c',
        'MAIL_TYPE': 'Mail_Type__c',
        'WT_RATE': 'Per_Lb__c',
        'PC_RATE': 'Per_Pc__c',
    }

    transforms = {
        'Max_Oz__c': lambda df: df['PC_WT_MAX'] * 16,
        'Min_Oz__c': lambda df: df['PC_WT_MIN'] * 16,
        'Min_Pieces__c': lambda df: 0
    }

    rates_df = (
        base_rates
            .rename(columns = renames)
            .assign(**transforms)
            [list(renames.values()) + list(transforms.keys())]
    )

    return rates_df.to_dict(orient='records')