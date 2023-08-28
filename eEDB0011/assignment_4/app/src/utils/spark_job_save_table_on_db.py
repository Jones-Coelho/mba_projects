import sys

from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# Author: Jones Coelho


class ETLJob:
    """
    A class that performs ETL (Extract, Transform, Load)
    """

    def __init__(self,
                 input_table:str="db_spec.tb_metrics",
                 output_table:str=None,
                 database:str=None,
                 host:str=None, 
                 user:str=None, 
                 password:str=None,
                 driver_path:str=None
                 ):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.input_table = input_table
        self.output_table = output_table
        
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.driver_path = driver_path

    def extract(self) -> dict:
        """
        Reads a CSV file from a specified location and
        returns a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the file contents.
        """
        dataframe =  self.spark\
                          .read\
                          .table(self.input_table)
                          
          
        return dataframe
    

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """
        Executes transformations on the input DataFrame based
        on business requirements.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame ready to be loaded.
        """
        
        return dataframe

    def load(self, dataframe: DataFrame) -> None:
        """
        Saves a DataFrame to a mysql database.

        Args:
            dataframe (DataFrame): The Spark DataFrame containing
            the data to be loaded.

        Returns:
            None
        """
        
        connection = {
                "url": f"jdbc:mysql://{self.host}:3306/{self.database}",
                "dbtable": self.output_table,
                "user": self.user,
                "password": self.password,
                "customJdbcDriverS3Path": self.driver_path,
                "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
                }
             
        glue_df = DynamicFrame.fromDF(dataframe, self.glueContext, self.table_name)
        self.glueContext.write_from_options(frame_or_dfc=glue_df, 
                                                connection_type="mysql", 
                                                connection_options=connection)
        

    def run(self) -> None:
        """
        Executes the complete ETL process.

        Args:
            None

        Returns:
            None
        """
        self.load(self.transform(self.extract()))

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                          ["JOB_NAME",
                           "INPUT_TABLE",
                           "OUTPUT_TABLE",
                           "DATABASE",
                           "DB_HOST",
                           "DB_USER",
                           "DB_PASSWORD",
                           "DRIVER_PATH"
                           ])
    
    ETL: ETLJob = ETLJob(input_table=args.get("INPUT_TABLE"),
                         output_table=args.get("OUTPUT_TABLE"),
                         database=args.get("DATABASE"),
                         host=args.get("DB_HOST"),
                         user=args.get("DB_USER"),
                         password=args.get("DB_PASSWORD"),
                         driver_path=args.get("DRIVER_PATH")
                         )
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()