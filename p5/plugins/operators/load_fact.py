from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Airflow Operator class to load data into the fact tables
    Args:
        table (str): The Redshift staging table name
        redshift_conn_id (str): The AWS Redshift Connection Identificator
        load_sql_stmt (str): The SQL code to load the data
"""

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt
        

    def execute(self, context):
        self.log.info(f"Starting to execute LoadFactOperator")
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        
        sql_command = LoadFactOperator.insert_sql.format(self.table,self.load_sql_stmt)
        
        self.log.info(f"Starting to load Fact Table")
        redshift.run(sql_command)
        
        
