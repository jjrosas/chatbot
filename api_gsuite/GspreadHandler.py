"""
    Gspread handler class.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.utils import a1_range_to_grid_range
from typing import Union
import pandas as pd

def get_id_from_url(url:str):

    return url.split('/edit')[0].split('d/')[-1]

class GspreadHandler:

    """

    Class gspread_handler

        A class to edit google sheets
    ...
    Attributes
    ----------
        json_key_file : str
            Path to Google's API key json file
    Methods
    -------
        get_sheet_from_gsheet(sheet_id,sheet_name):
            Returns a Google Sheets worksheets object.
            sheet_id: str
                String id of the sheet (available in the file url)
            sheet_name: str
                Sheet name

        get_df_from_gsheet(sheet_id,sheet_name):
            Returns a pandas.DataFrame from a google sheet object
            sheet_id: str
                string id of the sheet (available in the file url)
            sheet_name: str
                Sheet name
        update_sheet(sheet_id,sheet_name,df,add_headers = False):
            Writes the information of a dataframe in a google sheets
            sheet_id: str
                string id of the sheet (available in the file url)
            sheet_name: str
                Sheet name
            df: pandas.dataframe
                Dataframe that will be uploaded to google sheet
            add_headers: bool
                Dataframe that will be uploaded to google sheet
        clean_sheet(self,sheet_id,sheet_name):
            clean the content of a sheet
                sheet_id: str
                    string id of the sheet (available in the file url)
                sheet_name: str
                    Sheet name
                clean headers: bool
                    If True, headers of the table will be cleaned
    """

    def __init__(self,json_key_file:Union[str,dict]):

        SCOPES = ['https://www.googleapis.com/auth/drive']

        if type(json_key_file) is str:

            CREDS = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, SCOPES)

        elif type(json_key_file) is dict:

            CREDS  = ServiceAccountCredentials.from_json_keyfile_dict(json_key_file,SCOPES)

        self.client = gspread.authorize(CREDS)

    def get_sheet_name(self,sheet_id_or_url:str)->str:

        #Workbook
        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        wb = self.client.open_by_key(sheet_id)
        return wb.title

    def get_sheet_from_gsheet(self,sheet_id_or_url:str,sheet_name:str):
        """
        Returns a Google Sheets worksheets object.
        sheet_id: str
                String id of the sheet (available in the file url)
        sheet_name: str
                Sheet name
        """

        #Workbook
        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        wb = self.client.open_by_key(sheet_id)

        #Sheets a la que inserto un rango
        ws = wb.worksheet(sheet_name)

        return ws

    def list_workseets(self,sheet_id_or_url:str):

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        ws =[x.title for x in  self.client.open_by_key(sheet_id).worksheets()]
        return ws


    def get_df_from_gsheet(self,sheet_id_or_url:str,sheet_name:str,as_object=False)->pd.DataFrame:

        """
        Returns a pandas.DataFrame from a google sheet object
        sheet_id: str
            string id of the sheet (available in the file url)
        sheet_name: str
            Sheet name
        """

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        ws = self.get_sheet_from_gsheet(sheet_id,sheet_name)

        if as_object:
            values = ws.get_all_values()
            df = pd.DataFrame(values[1:],dtype = 'object',columns=values[0])
        else:
            df = pd.DataFrame(ws.get_all_records())
        return df

    def update_sheet(self,sheet_id_or_url:str,sheet_name:str,df:pd.DataFrame,add_headers = False):

        """
        Writes the information of a dataframe in a google sheets
            sheet_id: str
            sheet_name: str
            df: pandas.dataframe
            add_headers: bool
        """

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        ws = self.get_sheet_from_gsheet(sheet_id,sheet_name)

        df = df.fillna('')

        for col in df.columns:
            df[col] = df[col].astype(str)

        if add_headers:

            update_values = [df.columns.tolist()]+df.values.tolist()

            df = pd.DataFrame(update_values)

        else:
            update_values = df.values.tolist()

        top_left_cell = 'A1' if add_headers else 'A2'

        rc_init = a1_range_to_grid_range(top_left_cell)

        init_row,init_col = rc_init['endRowIndex'],rc_init['endColumnIndex']

        row, col= df.shape

        row+=(init_row-1) ; col+=(init_col-1)

        ws.update(f'R{init_row}C{init_col}:R{row}C{col}',
                            update_values,
                            value_input_option = 'USER_ENTERED')

        print(f'sheet {sheet_name} was updated')

    def clean_sheet(self,sheet_id_or_url:str,sheet_name:str):

        """
        clean the content of a sheet
            sheet_id: str
                string id of the sheet (available in the file url)
            sheet_name: str
                Sheet name
            clean headers: bool
                If True, headers of the table will be cleaned
        """

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url
        ws = self.get_sheet_from_gsheet(sheet_id,sheet_name)

        nrows =  len(ws.get_all_records())+1

        if nrows >1:

            ncols = len(ws.get_all_values()[0])+1

            update_values = [['']*ncols]*nrows

            top_left_cell = 'A1'

            rc_init = a1_range_to_grid_range(top_left_cell)

            init_row,init_col = rc_init['endRowIndex'],rc_init['endColumnIndex']

            row, col= nrows,ncols

            row=init_row+nrows ; col=init_col+ncols

            ws.update(f'R{init_row}C{init_col}:R{row}C{col}',
                            update_values,
                            value_input_option = 'USER_ENTERED')


        print(f'sheet {sheet_name} was cleaned')

    def copy_worksheet(self,sheet_id_or_url:str,ws_new_title:str,copy_permissions=True):

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url

        return self.client.copy(sheet_id,ws_new_title,copy_permissions)

    def add_worksheet(self,sheet_id_or_url:str,sheet_name:str):

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url

        return self.client.open_by_key(sheet_id).add_worksheet(title=sheet_name, rows="100", cols="20")

    def get_worksheets(self,sheet_id_or_url:str):

        sheet_id = get_id_from_url(sheet_id_or_url) if 'http' in sheet_id_or_url else sheet_id_or_url

        wb = self.client.open_by_key(sheet_id)

        return list(wb.worksheets())

