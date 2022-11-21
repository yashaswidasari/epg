import pandas as pd
from typing import Dict
from abc import abstractmethod, ABC


class MirrorsTransformer(ABC):
    @abstractmethod
    def transform_tariff(mirrors_map:Dict, tariff:pd.DataFrame) -> pd.DataFrame:
        pass