import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logging.info('Running data quality tests')
        len_dq_checks = len(self.dq_checks)
        for i, test in enumerate(self.dq_checks):
            test_query = test['test_query']
            operator = test['operator']
            value = test['value']
             
            if operator not in ['greater_than', 'smaller_than', 'equal_to']:
                raise Exception(f'Operator {operator} is not accepted')
            
            logging.info(f'Running test query: {test_query} \
                with expected result: {operator} {value}')
            
            records = redshift.get_records(test_query)
            result = records[0][0]
            
            if operator == 'greater_than':
                if not result > value:
                    raise ValueError(f'Result of query is {result} and is not \
                        {operator} {value}')
            elif operator == 'smaller_than':
                if not result < value:
                    raise ValueError(f'Result of query is {result} and is not \
                        {operator} {value}')
            else:
                if not result == value:
                    raise ValueError(f'Result of query is {result} and is not \
                        {operator} {value}')
            
            logging.info(f'Data quality check {i}/{len_dq_checks} passed')
        
        logging.info('All tests have passed')