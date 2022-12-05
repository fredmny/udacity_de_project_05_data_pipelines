from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    template_fields = ('sql_query')
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_query='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading data into fact table')
        rendered_sql = self.sql_query.format(**context)
        slq_final = f'''
            INSERT INTO {self.table} (
                {rendered_sql}
            )
        '''
        redshift.run(slq_final)
