import boto3
import os
from dotenv import load_dotenv
from time import sleep

load_dotenv()

RUNNING_STATE = 'RUNNING'
FAILED_STATE = 'FAILED'
STOPPED_STATE = 'STOPPED'
SUCCESS_STATE = 'SUCCEEDED'

_JOB_LOG_OUPUT_URL = "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws-glue$252Fjobs$252Foutput$3FlogStreamNameFilter$3D{job_run_id}"
_JOB_LOG_ALL_URL   = "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/%2Faws-glue%2Fjobs%2Flogs-v2?logStreamNameFilter={job_run_id}"
_JOB_LOG_ERROR_URL = "https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logStream:group=/aws-glue/jobs/error;prefix={job_run_id};streamFilter=typeLogStreamPrefix"
_JOB_CONSOLDE_URL = "https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/etl_primary_load/{job_name}"

JOB_LOG_DICT_URL = {
    'All logs' : _JOB_LOG_ALL_URL,
    'Output logs' : _JOB_LOG_OUPUT_URL,
    'Error logs': _JOB_LOG_ERROR_URL
}

class GlueHandler:

    def __init__(self,aws_access_key_id=None,aws_secret_access_key=None) -> None:

        """
        Starts the Glue client handler
        """

        self.client = boto3.client('glue',
                            aws_access_key_id=aws_access_key_id or os.environ.get('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=aws_secret_access_key or os.environ.get('AWS_SECRET_ACCESS_KEY')
                            )

    def get_table_info(self,database_name:str,table_name:str):

        table_info = self.client.get_table(
            DatabaseName=database_name,
            Name=table_name,
        )

        return table_info

    def _generate_job_args(self,job_spec,GLUE_TOKEN):

        job_args = {}
        job_args['Name'] = job_spec.get('name')
        job_args['Description'] = ''
        job_args['Role'] = job_spec.get('role')
        job_args['ExecutionProperty'] = {'MaxConcurrentRuns':1}
        job_args['Command'] = {
            'Name':'glueetl',
            'ScriptLocation':job_spec.get('command').get('scriptLocation'),
            'PythonVersion':job_spec.get('command').get('pythonVersion')
        }
        job_args['Connections'] = {'Connections':job_spec.get('connections').get('connections')}
        job_args['SourceControlDetails'] = {
                'Provider': 'GITHUB',
                'Repository': 'nocnoc_glue',
                'Owner': 'nocnocgroup',
                'Branch': 'main',
                'Folder': 'elt_primary_load',
                'AuthStrategy': 'PERSONAL_ACCESS_TOKEN',
                'AuthToken': GLUE_TOKEN
            }
        job_args['DefaultArguments'] = {
        '--enable-metrics': 'true',
        '--enable-spark-ui': 'true',
        '--extra-py-files': 's3://bi-data-nocnoc/glue_extra_files/glue_utils.zip',
        '--spark-event-logs-path': 's3://aws-glue-assets-309575297135-us-east-1/sparkHistoryLogs/',
        '--enable-job-insights': 'false',
        '--enable-observability-metrics': 'true',
        '--enable-glue-datacatalog': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--job-bookmark-option': 'job-bookmark-enable',
        '--job-language': 'python',
        '--TempDir': 's3://aws-glue-assets-309575297135-us-east-1/temporary/'
            }

        job_args['MaxRetries'] = 0
        job_args['Timeout'] = 30
        job_args['GlueVersion'] = '4.0'
        job_args['NumberOfWorkers'] = 2
        job_args['WorkerType'] = 'G.1X'

        return job_args

    def create_job(self,job_spec,GLUE_TOKEN):

        self.client.create_job(
            **self._generate_job_args(job_spec,GLUE_TOKEN)
        )


    def get_job_runs(self,job_name:str):

        """
        List the jobs runs for a give job_name
        """

        job_runs = []

        response = self.client.get_job_runs(JobName=job_name)

        job_runs.extend(response.get('JobRuns'))

        while 'NextToken' in response:
            response = self.client.get_job_runs(JobName=job_name,NextToken=response.get('NextToken'))
            job_runs.extend(response.get('JobRuns'))

        return job_runs


    def list_jobs(self,next_token=None):

        """
        List all the jobs in the account
        """

        if next_token:
            jobs = self.client.list_jobs(NextToken=next_token,MaxResults=100)
        else:
            jobs = self.client.list_jobs(MaxResults=100)

        return jobs


    def get_job(self,job_name):

        """
        Get the details for a job name
        """

        return self.client.get_job(JobName=job_name)

    def update_source_control_from_job(self,
                                       job_name,
                                       auth_token=None,
                                       provider="GITHUB",
                                       repository_name="nocnoc_glue",
                                       repository_owner="nocnocgroup",
                                       branch_name="main",
                                       folder="jobs",
                                       auth_strategy="PERSONAL_ACCESS_TOKEN"
                                       ):

        """
        This updates the job in the Github Repo
        """

        if auth_token is None:
            raise Exception("AuthToken should be provided")

        response = self.client.update_source_control_from_job(
                JobName=job_name,
                Provider=provider,
                RepositoryName=repository_name,
                RepositoryOwner=repository_owner,
                BranchName=branch_name,
                Folder=folder,
                AuthStrategy=auth_strategy,
                AuthToken=auth_token
        )

    def update_job_from_source_control(self,
                                       job_name,
                                       provider="GITHUB",
                                       repository_name="nocnoc_glue",
                                       repository_owner="nocnocgroup",
                                       branch_name="main",
                                       folder="jobs",
                                       auth_strategy="PERSONAL_ACCESS_TOKEN",
                                       auth_token=None):

        """
        This updates the glue job source code from Github
        """

        if auth_token is None:
            raise Exception("AuthToken should be provided")

        response = self.client.update_job_from_source_control(
                JobName=job_name,
                Provider=provider,
                RepositoryName=repository_name,
                RepositoryOwner=repository_owner,
                BranchName=branch_name,
                Folder=folder,
                AuthStrategy=auth_strategy,
                AuthToken=auth_token
        )


    def start_job_run(self,job_name:str,arguments=None,worker_type=None,number_of_workers=None):

        params = {}
        params['JobName'] = job_name
        if arguments:
            params['Arguments'] = arguments
        if worker_type:
            valid_worker_type = ['Standard','G.1X','G.2X','G.025X','G.4X','G.8X','Z.2X']
            if worker_type not in valid_worker_type:
                raise Exception(f'{worker_type} is not a valid worker type')

            params['WorkerType'] = worker_type

        if number_of_workers:
            params['NumberOfWorkers'] = number_of_workers

        return self.client.start_job_run(
            **params
        )


    def get_job_run(self,job_name:str,job_run_id:str,predecessors_included=False):

        return self.client.get_job_run(
                JobName=job_name,
                RunId=job_run_id,
                PredecessorsIncluded=predecessors_included
            )

    def _print_log_urls(self,job_run_id:str):

        print('Cloudwatch logs:')

        for key , item in JOB_LOG_DICT_URL.items():
            print('-',key,':',item.format(job_run_id=job_run_id))

    def _print_job_console_url(self,job_name):

        print('Job Console:',_JOB_CONSOLDE_URL.format(job_name=job_name))

    def trigger_job_and_wait(self,job_name,**kwargs):

        job_run = self.start_job_run(job_name=job_name,**kwargs)

        job_run_id = job_run.get('JobRunId')

        current_status = 'RUNNING'

        self._print_job_console_url(job_name)
        self._print_log_urls(job_run_id)

        while current_status == RUNNING_STATE:

            current_status = self.get_job_run(job_name,job_run_id).get('JobRun').get('JobRunState')

            sleep(5)

        if current_status == FAILED_STATE:
            raise Exception(self.get_job_run(job_name,job_run_id).get('JobRun').get('ErrorMessage'))

        if current_status == STOPPED_STATE:
            print('Job has been stopped')

        if current_status == SUCCESS_STATE:
            print(f'Job {job_name} completed ')


    def get_job_bookmark(self,job_name):
        return self.client.get_job_bookmark(JobName=job_name)

    def get_connection_properties(self,connection_name):

        return self.client.get_connection(Name=connection_name).get('Connection').get('ConnectionProperties')

    def get_workflow(self,workflow_name):

        return self.client.get_workflow(Name=workflow_name,IncludeGraph=True)

    def list_crawlers(self):

        return self.client.list_crawlers(MaxResults=100)

    def get_crawler(self,crawler_name):
        return self.client.get_crawler(Name=crawler_name)

    def get_triggers(self,JobName:str):
        return self.client.list_triggers(DependentJobName=JobName)

    def get_trigger_details(self,TriggerName:str):
        return self.client.get_trigger(Name=TriggerName)

    def list_triggers(self):
        return self.client.list_triggers(MaxResults=100)
