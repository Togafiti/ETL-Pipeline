from typing import Callable, Dict, Optional

import pandas as pd

from .operators import clean_operators
from .stations import clean_stations
from .station_traffic import clean_station_traffic

CleanerFunc = Callable[[pd.DataFrame], pd.DataFrame]

CLEANER_REGISTRY: Dict[str, CleanerFunc] = {
    "operators": clean_operators,
    "stations": clean_stations,
    "station_traffic": clean_station_traffic,
}


def get_cleaner(table_name: str) -> Optional[CleanerFunc]:
    """Return cleaner function for table if registered."""
    return CLEANER_REGISTRY.get(table_name)
