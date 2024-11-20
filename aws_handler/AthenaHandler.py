from database_credentials import s3_creds
import boto3
import pandas as pd
from tqdm import tqdm
from time import sleep

class AthenaHandler:

    def __init__(self) -> None:
        """
        Starts the athena client
        """

        self.client = boto3.client('athena',
                            aws_access_key_id=s3_creds['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=s3_creds['AWS_SECRET_ACCESS_KEY'],
                            region_name = 'us-east-1')

    def get_catalog(self):

        """
        list the athena catalog
        """

        paginator = self.client.get_paginator('list_data_catalogs')

        return list(paginator.paginate())[0].get('DataCatalogsSummary')

    def get_databases(self,catalog_name='AwsDataCatalog'):

        """
        list the athena databases
        """

        paginator = self.client.get_paginator('list_databases')

        return list(paginator.paginate(CatalogName=catalog_name))[0].get('DatabaseList')


    def get_tables_metadata(self,catalog_name:str,database_name:str):
        """
        returns metadata of a givent able
        """

        return self.client.list_table_metadata(
        CatalogName=catalog_name,
        DatabaseName= database_name
        ).get('TableMetadataList')

    def _start_query(self,catalog_name:str,database_name:str,query:str):

        response = self.client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': catalog_name,
            'Catalog': database_name
        },
        ResultConfiguration={
                'OutputLocation': 's3://nocnoc-data-input/timestream-temp',
                })

        return response['QueryExecutionId']

    def get_metadata_all_tables(self,as_pandas=True):

        metadata_info = {}

        databases =self.get_databases()

        for database in tqdm(databases):
            database_name = database.get('Name')
            metadata_info[database_name] = self.get_tables_metadata(
                                            self.get_catalog()[0]['CatalogName'],
                                            database_name)

        if not as_pandas:
            return metadata_info

        list_df = []
        for key, item in metadata_info.items():
            list_df.append(
                pd.json_normalize(item).assign(database_name=key)
            )

        return pd.concat(list_df)

    def _get_query_status(self,query_execution_id):

        response = self.client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        return response['QueryExecution']['Status']['State']

    def _convert_raw_to_df(self,rows):

        columns = [x.get('VarCharValue') for x in rows[0]]

        df = pd.json_normalize(rows[1:])

        if df.empty:
            return df

        df.columns = columns

        for col in columns:
            df[col] = df[col].apply(lambda x: x.get('VarCharValue'))

        return df

    def _get_query_results_with_paginator(self,query_execution_id:str):
        results_paginator = self.client.get_paginator('get_query_results')
        results_iter = results_paginator.paginate(
            QueryExecutionId=query_execution_id,
            PaginationConfig={
                'PageSize': 1000
                }
            )

        list_results = []

        for response in results_iter:
            list_results.extend(
                [x.get('Data') for x in response['ResultSet']['Rows']]
            )

        df = self._convert_raw_to_df(list_results)

        return df

    def _get_query_results(self,query_execution_id:str):

        response = self.client.get_query_results(
            QueryExecutionId=query_execution_id
        )

        rows = [x.get('Data') for x in response['ResultSet']['Rows']]

        return self._convert_raw_to_df(rows)

    def run_query(self,query:str,use_paginator=False):

        query_execution_id = self.client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
            'OutputLocation': 's3://nocnoc-data-input/timestream-temp'
            }
        )['QueryExecutionId']

        query_current_status = self._get_query_status(query_execution_id)

        while query_current_status in ['RUNNING','QUEUED']:
            sleep(5)
            query_current_status = self._get_query_status(query_execution_id)

        if self._get_query_status(query_execution_id)=='FAILED':
            raise Exception(
                self.client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['StateChangeReason']
            )

        if self._get_query_status(query_execution_id)=='SUCCEEDED':
            if not use_paginator:
                return self._get_query_results(query_execution_id)
            return self._get_query_results_with_paginator(query_execution_id)


    def delete_tables(self,list_delete_tables):

        list_executions = []

        for table_to_delete in tqdm(list_delete_tables):

            query_execution_id = self.client._start_query('AwsDataCatalog',
                                                    'datalake_raw',
                                                    f'drop table if exists datalake_raw.{table_to_delete}')

            list_executions.append(query_execution_id)

        return list_executions


if __name__=="__main__":
    ok_query =     """
            with order_service_id as (
            SELECT order_id,
                cast(json_extract(request,'$.service_id') as int) as service_id,
                date_time,
                row_number() over(partition by order_id order by date_time desc) as row_num
            FROM "AwsDataCatalog"."communication-records-prod".communication_records_prod
        )
        select order_id,
                service_id,
                date_time
        from order_service_id
        where row_num=1
        """



    results = AthenaHandler().run_query(
        ok_query, use_paginator=True
    )

    results.to_csv('comm_records_prod.csv')