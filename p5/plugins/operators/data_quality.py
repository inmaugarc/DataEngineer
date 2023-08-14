from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 dq_checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Starting to execute DataQualityOperator ...')
        
        if len(self.dq_checks)<=0:
                self.log.info(f"There is no Data Check!!!!")
                return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        n_errors = 0
        error_tests = []
        
        for check in self.dq_checks:
            test = check.get('check_sql')
            exp_result_test = check.get('expected_result')
            
            try:
                self.log.info(f"Starting to execute query {sql}")
                result = redshift_hook.get_records(sql)[0]
                
            except Exception as e:
                self.log.info(f"Query crashed giving the exception {e}")
            
            if exp_result_test != result[0]:
               n_errors +=1
               error_tests.append(sql) 
        
        if n_errors >0:
            self.log.info(f"Tests crashed")
            self.log.info(error_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info("Data quality check run successfully")
                
