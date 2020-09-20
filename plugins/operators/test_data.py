from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class TestDataOperator(BaseOperator):
    ui_color = '#8CCD9E'

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 expected=0,
                 *args, **kwargs):

        super(TestDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql
        self.expected=expected
        
    def execute(self, context):
        self.log.info('TestDataOperator Begin Executing')
        
        redshiftHook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshiftHook.get_records(f"{self.sql}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"case check failed. {self.sql} has no values")
        
        result = records[0][0]
        
        if result != self.expected:
            raise ValueError(f"case check failed. {self.sql} not equal to {self.expected}")
        
        self.log.info(f"quality on case {self.sql} check passed with {self.expected} value")
