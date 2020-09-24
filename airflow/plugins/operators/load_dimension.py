from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    append_sql="""
                    BEGIN;
                    INSERT INTO {table}
                    {sql};
                    COMMIT;"""
    
    replace_sql="""
                    BEGIN;
                    TRUNCATE TABLE {table}; 
                    INSERT INTO {table}
                    {sql};
                    COMMIT;"""
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 is_append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.is_append = is_append

    def execute(self, context):
        self.log.info('LoadDimensionOperator Start Executing')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        insert_sql = ""
        self.log.info(f"Loading dimension table {self.table}")
        
        if self.is_append :
            insert_sql = self.append_sql.format(table=self.table,sql= self.sql_stmt)
        else:
            insert_sql = self.replace_sql.format(table= self.table,sql= self.sql_stmt)
        
        self.log.info(f"Inserting into dimension table {self.table} as {insert_sql}")
        redshift.run(insert_sql)
        
        self.log.info(f"Inserting into dimension table {self.table} finished")