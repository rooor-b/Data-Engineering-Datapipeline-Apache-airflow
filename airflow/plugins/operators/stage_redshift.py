from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    """ templated copy sql
    """
    copy_sql = """
       COPY {table}
       FROM '{s3_path}'
       with credentials
       'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
       IGNOREHEADER {ignore_headers}
       json 'auto ignorecase'
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 ignore_headers=0,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id = aws_credentials_id 
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is running')
        
        self.s3 = S3Hook(aws_conn_id=self.aws_credentials_id, verify=None)
        
        credentials = self.s3.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_bucket_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        redshiftHook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        staging_sql = StageToRedshiftOperator.copy_sql.format(
                   table=self.table,
                   s3_path=s3_bucket_path,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   ignore_headers=self.ignore_headers)
        
        self.log.info(f'Executing {staging_sql}')
        self.log.info(f'Executing Staging to redshift {self.table} from S3')
        
        redshiftHook.run(staging_sql)
        
        self.log.info(f"COPY {self.table} complete")





