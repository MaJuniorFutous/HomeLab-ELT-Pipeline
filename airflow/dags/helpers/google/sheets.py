from typing import Optional, Union

import pandas as pd
import gspread

# from utils import create_google_sa_obj
from .utils import create_google_sa_obj
from .creds.creds import SCOPES, CREDS


def create_worksheet_obj(
        spreadsheet: str, 
        key: Optional[str] = None, 
        url: Optional[str] = None,
        sheet1: bool = True,
        ws_name: Optional[Union[str, int]] = None):
    #TODO: Impliment url and key methods and priorties for each
    _gspread = gspread.authorize(create_google_sa_obj(creds=CREDS, scopes=SCOPES))
    ss = _gspread.open(spreadsheet)
    if sheet1 and (ws_name or key or url):
        print("Both sheet1 and worksheet name was passed. Using sheet1...")
    if sheet1: return ss.sheet1
    elif ws_name:
        if isinstance(ws_name, str): return ss.worksheet(ws_name)
        if isinstance(ws_name, int): return ss.get_worksheet(ws_name)

def read_sheet(ws_obj, pandas: bool = False, columns: Optional[Union[bool, list]] = True):
    if columns and not pandas:
        print("Note: headers passed but not pandas.")
    data = ws_obj.get_all_values()  # list of list (2 dim matrix)
    if pandas:
        if columns:
            if isinstance(columns, bool): return pd.DataFrame(data[1:], columns=data[0])
            else: return pd.DataFrame(data, columns=columns)
    else: return data

