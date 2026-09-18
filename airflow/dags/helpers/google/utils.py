from typing import Optional, Union, List

import gspread
from google.oauth2 import service_account


def create_google_sa_obj(creds: Union[dict, str], scopes: Optional[List[str]] = None):
    if isinstance(creds, dict): credentials = service_account.Credentials.from_service_account_info(creds)
    else: credentials = service_account.Credentials.from_service_account_file(creds)
    return credentials.with_scopes(scopes) if scopes else credentials