
import pandas as pd
import requests as r
from google.auth.transport import requests
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from typing import Union
from oauth2client.service_account import ServiceAccountCredentials

def create_ga_service(creds,SCOPES,service_name='analyticsreporting',version='v4'):
    
    """
    Builds the GA service depending on the class
    """
    
    try:
        print('trying as service account')
        if isinstance(creds,str):
            creds = service_account.Credentials.from_service_account_file(creds,scopes=SCOPES)
        elif isinstance(creds,dict):
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds,scopes=SCOPES)
        service = build(service_name, version, credentials=creds)
        print('trying as service account succeeded!')
        return service
        
    except:
        # Revisar si aplica para google Analytics
        print('trying as flow application failed')
        try:
            print('trying as service account')
            flow = InstalledAppFlow.from_client_secrets_file(creds, SCOPES)
            creds = flow.run_local_server(port=8090)
            service = build(service_name, version, credentials=creds)
            return service
        except:
            print('trying as flow application failed')
            pass


class GanalyticHandler():

    def __init__(self,creds:Union[str,dict],view_id:int):

        """
        Class for google analytic handling
        """

        SCOPES=['https://www.googleapis.com/auth/analytics.edit',
                'https://www.googleapis.com/auth/analytics.readonly']
        
        self.service = create_ga_service(creds,SCOPES)
        
        self.VIEW_ID = view_id

    def print_response(self,response):
        """
        Parses and prints the Analytics Reporting API V4 response.

        Args:
        response: An Analytics Reporting API V4 response.
        """
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    print(header + ': ' + dimension)

                for i, values in enumerate(dateRangeValues):
                    print('Date range: ' + str(i))
                    for metricHeader, value in zip(metricHeaders, values.get('values')):
                        print(metricHeader.get('name') + ': ' + value)

    def get_report(self,start_date:str,
                    end_date:str,
                    metrics:Union[str,list],
                    dimensions:Union[str,list],
                    filter=None):
        
        """
        get a report from google analytic
        :start_date: in YYYY-mm-dd format
        :end_date: in YYYY-mm-dd format
        :metrics: str or list of the metrics ref https://ga-dev-tools.web.app/
        :dimensions: str or list of dimensions ref https://ga-dev-tools.web.app/
        Best resource: https://ga-dev-tools.web.app/dimensions-metrics-explorer/
        returns a ga response
        """

        if isinstance(metrics,str):
            metrics=[metrics]
        metrics = [{'expression':x} for x in metrics]

        if isinstance(dimensions,str):
            dimensions=[dimensions]
        dimensions = [{'name':x} for x in dimensions]

        date_range = [{'startDate': start_date, 'endDate': end_date}]

        request_body =  {
            'viewId': f'{self.VIEW_ID}',
            'dateRanges': date_range ,
            'metrics': metrics,
            'dimensions': dimensions
            }

        if filter is not None:
            request_body["dimensionFilterClauses"] =  filter

        request_body = {'reportRequests': [request_body]}

        response = self.service.reports().batchGet(body=request_body).execute()
    
        return response

class GanalyticManagement():
    
    def __init__(self,creds:Union[str,dict]):
        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
                  'https://www.googleapis.com/auth/analytics.edit']

        self.service = create_ga_service(creds,SCOPES,'analytics','v3')

# TO DO: Handle unpack function
# TO DO: Handle multiple metrics