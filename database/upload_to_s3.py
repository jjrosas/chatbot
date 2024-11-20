import boto3
print( """TIP if you are using Tiendanube servers:
                to save a pickle/csv dataframe to S3 import s3fs and use the path as file destination.
                Example: df.to_pickle(s3:/tiendanube-data/my_folder/my_filename.pik)

        ELSE use this function (requires credentials)""")


def upload_file_s3(filename,bucket,s3_filename, aws_access_key_id, aws_secret_access_key):

    """
    Upload a file to de S3 bucket
        filename: str
            TODO ADD EXAMPLE
        bucket: str
            TODO ADD EXAMPLE
        s3_filename: str
            TODO ADD EXAMPLE
    """

    s3_client = boto3.client("s3",
                             aws_access_key_id,
                             aws_secret_access_key)

    s3_client.upload_file(Filename = filename,
                          Bucket = bucket,
                          Key = s3_filename)

    return "File uploaded"

#TODO upload from buffer using IO buffer (for plots, png files, joblib, etc.)
