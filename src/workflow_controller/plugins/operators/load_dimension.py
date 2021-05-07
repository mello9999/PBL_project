from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
            ({}) {};
    """

    @apply_defaults
    def __init__(self,
                 table = '',
                 fields = '',
                 redshift_conn_id = '',
                 load_dim_sql = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.fields = fields
        self.redshift_conn_id = redshift_conn_id
        self.load_dim_sql = load_dim_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
       
        self.log.info('Clearing data from Redshift table for new data')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f'Loading {self.table} dimensions in redshift')        
        dimensions_table_insert = LoadDimensionOperator.dimensions_table_insert.format(
            self.table,
            self.fields,
            self.load_dimension
        )

        redshift.run(dimensions_table_insert)
    
