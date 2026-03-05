from typing import Callable, Dict, Optional

import pandas as pd

from .orders import clean_orders
from .users import clean_users

CleanerFunc = Callable[[pd.DataFrame], pd.DataFrame]

CLEANER_REGISTRY: Dict[str, CleanerFunc] = {
    "orders": clean_orders,
    "users": clean_users,
}


def get_cleaner(table_name: str) -> Optional[CleanerFunc]:
    """Return cleaner function for table if registered."""
    return CLEANER_REGISTRY.get(table_name)
