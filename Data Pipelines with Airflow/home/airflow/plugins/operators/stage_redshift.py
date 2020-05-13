import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    StageToRedShiftCopyStmt = """
        copy {destination}
        from {source}
        ACCESS_KEY_ID '{{access_key}}'
        SECRET_ACCESS_KEY '{{secret_key}}'
        region {region}
        timeformat 'epochmillisecs'
        json 'auto';
        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id="",
                 source = "",
                 destination = "",
                 access_key = "",
                 secret_key = "",
                 region = "",
                 appendData = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.appendData = appendData
        self.source = source
        self.partitionDate = kwargs.get('execution_date')
        self.destination = destination
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.appendData:
            self.log.info('StageToRedshiftOperator copy data')
            copy_stmt = StageToRedshiftOperator.StageToRedShiftCopyStmt.format(
                source = self.source + ("/"+self.partitionDate.strftime("%Y") +"-"+self.partitionDate.strftime("%m")+"-"+self.partitionDate.strftime("%d")+".json"),
                destination = self.destination,
                access_key = self.access_key,
                secret_key = self.secret_key,
                region = self.region
            )
        else:
            self.log.info('StageToRedshiftOperator deleting data')
            redshift.run("DELETE * from {}".format(self.destination))
            self.log.info('StageToRedshiftOperator copy data')
            copy_stmt = StageToRedshiftOperator.StageToRedShiftCopyStmt.format(
                source = self.source,
                destination = self.destination,
                access_key = self.access_key,
                secret_key = self.secret_key,
                region = self.region
            )
        redshift.run(copy_stmt)






