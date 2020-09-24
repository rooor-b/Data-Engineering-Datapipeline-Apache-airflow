from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """BEGIN;
                    INSERT INTO {table} 
                    {sql_stmt};
                    COMMIT;
                 """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        
        self.log.info('LoadFactOperator Start Executing')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading fact table {self.table}")
        
        sql=self.insert_sql.format(table=self.table,sql_stmt=self.sql_stmt)
        
        self.log.info(f"Loading fact table {self.table} as {sql}")
        
