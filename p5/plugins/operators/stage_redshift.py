from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Airflow Operator class to copy data from S3 bucket into the Redshift staging tables
    Args:
        redshift_conn_id (str): The AWS Redshift Connection Identificator
        aws_credentials_id (str): The AWS credentials 
        table (str): The Redshift staging table name
        s3_bucket (str): The S3 bucket path
        s3_key (str): The S3 bucket name
        region (str): The region where the AWS bucket is stored
        more_params (str): In case we need extra parameters        


"""
    
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name                
                 redshift_conn_id="",                      
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 more_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.more_params = more_params
        
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgressHook(postgress_conn_id=self.redshift_conn_id)
        
        self.log.info('StageToRedshiftOperator being executed right now')   
        
        key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{key}"
        sql_stmt = StageToRedshiftOperator.copy_sql.format(     
            self.table, 
            credentials.access_key,
            credentials.secret_key,            
            s3_path,
            self.region,
            self.more_params
        )

        self.log.info(f"StageToRedshiftOperator: Executing query, copying data from the bucket '{s3_path}' to the table '{self.table}'")

        redshift.run(sql_stmt)




