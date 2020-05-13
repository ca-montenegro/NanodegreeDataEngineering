from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql_stmt = """
    INSERT INTO {destination} ({atributes}) ({sql_stmt})
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_stmt = "",
                 destination = "",
                 atributes = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.destination = destination
        self.atributes = atributes

    def execute(self, context):
        self.log.info('Loading Dimension Table'+ (self.destination))
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(LoadDimensionOperator.insert_sql_stmt.format(
            destination = self.destination,
            atributes = self.atributes,
            sql_stmt = self.sql_stmt))
