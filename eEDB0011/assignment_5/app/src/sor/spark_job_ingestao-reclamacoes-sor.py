import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
# Author: Jones Coelho


class ETLJob:
    """
    A class that performs ETL (Extract, Transform, Load)
    """

    def __init__(self, input_path, output_path):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.input_path: str = input_path
        self.output_path: str = output_path
        self.input_schema: StructType = StructType([
            StructField("Ano", StringType(), True),
            StructField("Trimestre", StringType(), True),
            StructField("Categoria", StringType(), True),
            StructField("Tipo", StringType(), True),
            StructField("CNPJ IF", StringType(), True),
            StructField("Instituição financeira", StringType(), True),
            StructField("Índice", StringType(), True),
            StructField("Quantidade de reclamações reguladas procedentes",
                        StringType(),
                        True),
            StructField("Quantidade de reclamações reguladas - outras",
                        StringType(),
                        True),
            StructField("Quantidade de reclamações não reguladas",
                        StringType(),
                        True),
            StructField("Quantidade total de reclamações",
                        StringType(),
                        True),
            StructField("Quantidade total de clientes  CCS e SCR",
                        StringType(), True),
            StructField("Quantidade de clientes  CCS", StringType(), True),
            StructField("Quantidade de clientes  SCR", StringType(), True)])

    def extract(self) -> DataFrame:
        """
        Reads a CSV file from a specified location and
        returns a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the file contents.
        """
        df_raw: DataFrame = self.spark\
                                .read\
                                .options(encoding="ISO-8859-1")\
                                .csv(path=self.input_path,
                                     sep=";",
                                     header=True,
                                     schema=self.input_schema,
                                     encoding="ISO 8859-1")
        return df_raw

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """
        Executes transformations on the input DataFrame based
        on business requirements.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame ready to be loaded.
        """
        dataframe = dataframe.withColumn("trimestre_temp",
                                         col=regexp_replace("Trimestre",
                                                              "º",
                                                              ""))
        dataframe = dataframe.withColumn("mes",
                                         F.when(F.col("trimestre_temp") == "1","03")
                                          .when(F.col("trimestre_temp") == "2","06")
                                          .when(F.col("trimestre_temp") == "3","09")
                                          .when(F.col("trimestre_temp") == "4","12")
                                          .otherwise("00").cast("string"))
        dataframe = dataframe.withColumn("anomesdia",
                                         F.concat(F.col("Ano"),F.col("mes"),F.lit("01")))
        return dataframe.drop(*["trimestre_temp", "mes"])

    def load(self, dataframe: DataFrame) -> None:
        """
        Saves a DataFrame to an output path as a Parquet file.

        Args:
            dataframe (DataFrame): The Spark DataFrame containing
            the data to be loaded.

        Returns:
            None
        """
        # Reorganize partitions and write a single file using coalesce
        # This is done to avoid generating too many small files,
        # which could degrade performance
        # on subsequent Spark jobs that read the output files.
        # In a production environment,
        # calculating the appropriate number of output files
        # should be considered.
        df_final: DataFrame = dataframe.coalesce(1)

        # Write data to output file in Parquet format with Snappy compression
        # If a file already exists at the output location,
        # it will be overwritten.
        df_final.write\
                .mode("overwrite")\
                .partitionBy("anomesdia")\
                .parquet(path=self.output_path,
                         compression="snappy")
        # Operação MSCK é custosa, em um ambiente real o
        # ideal é utilizar a SDK da AWS boto3 para adicionar
        # as partições diretamente no Glue Data Catalog
        self.spark.sql("MSCK REPAIR TABLE db_sor.tb_reclamacoes")

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
                              ['JOB_NAME',
                               'INPUT_PATH',
                               'OUTPUT_PATH'])
    ETL: ETLJob = ETLJob(input_path=args["INPUT_PATH"],
                         output_path=args["OUTPUT_PATH"])
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()