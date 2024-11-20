import warnings
import s3fs
import boto3 #for TODO
import pandas as pd

def read_from_s3(filepath, bucket = 'tiendanube-data', filetype = 'pik', local = False, aws_access_key_id = None, aws_secret_access_key= None):
    """
    Read a pickle file saved in S3 as a dataframe
    filepath: str
        string with a complete path to a piclkle file in s3
    bucket: str
        S3 bucket name, default = tiendanube-data
    filetype: str
        Specifies the filetype to load
            pik: pickle files
            csv: csv files
            parquet: parquet files
    local: bool
        If True you must provide credentials to S3. Else credentials are not needed (#TODO)
    aws_access_key_id: str
    aws_secret_access_key: str

    If you are in the server:
    To save a pickle/csv dataframe to S3 import s3fs and use the path as file destination.
    Example: df.to_pickle('s3://tiendanube-data/my_folder/my_filename.pik')


    Locally:
    Use upload_to_s3 function (needs credentials)
    """
    filename = f's3://{bucket}/{filepath}'
    if local:
        #TODO
        warnings.warn('Not implemented yet')
    else:
        fs = s3fs.S3FileSystem(anon=False)
        with fs.open(filename) as f:
            if filetype == 'pik':
                return pd.read_pickle(f)
            elif filetype == 'csv':
                return pd.read_csv(f)
            elif filetype == 'parquet':
                return pd.read_parquet(f)
            else:
                raise Exception('Invalid filetype')
