import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tables_with_pk={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_with_pk = tables_with_pk

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        logging.info('Running data quality tests')
        for table in self.tables_with_pk:
            logging.info(f'Testing if {table} has data')
            
            records = redshift.run(f'SELECT COUNT(*) FROM {table}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'{table} returned no resuts')

            num_records = records[0][0]
            if num_records == 0:
                raise ValueError(f'{table} contains 0 rows')

            records_unique = redshift.run(
                f'SELECT MAX(COUNT(DISTINCT {self.tables_with_pk(table)}))\
                    FROM {table}'
            )
            if records_unique[0][0] > 1:
                raise ValueError(f'{table} contains duplicate rows')
            
            logging.info(f'Data quality tests for {table} have passed')
        
        logging.info('All tests have passed')