from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_query='',
                 table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Deleting data from {self.table} table')
        redshift.run(f'DELETE FROM {self.table}')
        
        self.log.info(f'Loading data into {self.table} dimension table')
        final_sql = f'''
            INSERT INTO {self.table} (
                {self.sql_query}
            )
        '''
        redshift.run(final_sql)
        self.log.info(f'Successfully loaded data into \
            {self.table} dimension table')

