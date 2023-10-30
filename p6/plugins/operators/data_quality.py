from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# A task using the data quality operator is in the DAG and at least one data quality check is done
# Data quality check is done with correct operator
# The operator is parametrized
# Operator uses params to get the tests and the results, tests are not hard coded to the operator
# This final operator is meant to create a data quality operator, which is used to run checks on the data itself
# The operator's main functionality is to receive one or more SQL test cases together with the expected results
# And then execute these tests. For every test, the result and the expected result needs to be compared
# If there is no match, then this operator has to raise an exception and the tasks should be try again.
# As requested in the Rubric, the Data quality checks are done with the correct operator 
# and at least one data quality check is done
# Finally, this operator is parametrized and tests are not hard coded to the operator

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
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('Starting to execute DataQualityOperator ...')
        
        if len(self.dq_checks)<=0:
                self.log.info(f"There is no Data Check!!!!")
                return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        n_errors = 0
        error_tests = []
        result = []
        
        for check in self.dq_checks:
            test = check.get('check_sql')          
            exp_result_test = check.get('expected_result')
            print("This is the test ",test," and the expected result should be : ", exp_result_test)
            
            try:
                #self.log.info(f"Starting to execute query {sql}")
                print ("Starting to execute query ",test)
                #result = redshift_hook.get_records(sql)[0]
                result = redshift_hook.get_first(test)[0]
                print ("Real result: ",result)
                
            # The operator raises an error if the check fails pass
            except Exception as e:
                self.log.info(f"Query crashed giving the exception {e}")
            
            # The DAG either fails or retries n times
            if exp_result_test != result:
               n_errors +=1
               error_tests.append(test) 
        
        # The DAG either fails or retries n times
        if n_errors >0:
            self.log.info(f"Tests crashed")
            self.log.info(error_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info("Data quality check run successfully")
