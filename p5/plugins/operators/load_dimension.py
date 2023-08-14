from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
"""
    Operator class to load data into the dimension tables
    Args:
        table (str): The Redshift staging table name
        redshift_conn_id (str): The AWS Redshift Connection Identificator
        load_sql_stmt (str): The SQL code to load the data
        truncate_table (str): The way to insert data: it can be truncate (True) or add (False)
"""

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table
        

    def execute(self, context):
        self.log.info('LoadDimensionOperator starting to execute')
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table...")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table)
        
        self.log.info(f"Starting to load dimension table ...")
        sql_command = LoadDimensionOperator.insert_sql.format(self.table,self.load_sql_stmt)
        
        self.log.info(f"Executing Insert Query")
        redshift.run(sql_command)
