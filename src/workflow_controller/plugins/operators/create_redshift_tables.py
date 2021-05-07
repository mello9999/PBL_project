from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateRedshiftTablesOperator(BaseOperator):

    ui_color = '#2f6537'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 create_table_sql='',
                 *args, **kwargs):

        super(CreateRedshiftTablesOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql = create_table_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
            
            self.log.info(f'Creating {self.table} table in redshift')
            redshift.run(self.create_table_sql)