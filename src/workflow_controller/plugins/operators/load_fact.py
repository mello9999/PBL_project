from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
  
    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
            ({}) {};
    """

    @apply_defaults
    def __init__(self,
                 table = '',
                 fields = '',
                 redshift_conn_id = '',
                 load_facts_sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.fields = fields
        self.redshift_conn_id = redshift_conn_id
        self.load_facts_sql = load_facts_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Clearing data from Redshift table for new data')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Loading facts data into redshift facts_table')
        facts_table_insert = LoadFactOperator.insert_sql.format(
            self.table,
            self.fields,
            self.load_facts_sql
        )

        redshift.run(facts_table_insert)
