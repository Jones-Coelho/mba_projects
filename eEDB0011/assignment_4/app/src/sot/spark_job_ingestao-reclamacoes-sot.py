import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F
# Author: Jones Coelho


class ETLJob:
    """
    A class that performs ETL (Extract, Transform, Load)
    """

    def __init__(self, input_table, output_table):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.input_table: str = input_table
        self.output_table: str = output_table

    def extract(self) -> DataFrame:
        """
        Reads a CSV file from a specified location and
        returns a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the file contents.
        """
        df_raw: DataFrame = self.spark\
                                .read\
                                .table(self.input_table)
        return df_raw
    
    def _parse_columns(self, dataframe: DataFrame) -> DataFrame:
        parser: dict = {
            "ano": {
                "type": "int",
                "name": "ano"
            },
            "trimestre": {
                "type": "int",
                "name": "num_trimestre"
            },
            "categoria": {
                "type": "string",
                "name": "nom_categoria"
            },
            "tipo": {
                "type": "string",
                "name": "nom_tipo_instituicao"
            },
            "cnpj if": {
                "type": "bigint",
                "name": "num_cnpj_instituicao"
            },
            "instituição financeira": {
                "type": "string",
                "name": "nom_instituicao"
            },
            "índice": {
                "type": "float",
                "name": "vlr_ind_recl"
            },
            "quantidade de reclamações reguladas procedentes": {
                "type": "bigint",
                "name": "qtd_recl_reg_prec"
            },
            "quantidade de reclamações reguladas - outras": {
                "type": "bigint",
                "name": "qtd_recl_reg_outr"
            },
            "quantidade de reclamações não reguladas": {
                "type": "bigint",
                "name": "qtd_recl_nao_reg"
            },
            "quantidade total de reclamações": {
                "type": "bigint",
                "name": "qtd_totl_recl"
            },
            "quantidade total de clientes  ccs e scr": {
                "type": "bigint",
                "name": "qtd_totl_clie_css_scr"
            },
            "quantidade de clientes  ccs": {
                "type": "bigint",
                "name": "qtd_clie_ccs"
            },
            "quantidade de clientes  scr": {
                "type": "bigint",
                "name": "qtd_clie_scr"
            },
            "anomesdia": {
                "type": "string",
                "name": "anomesdia"
            }
        }
        dataframe = dataframe.select([c for c in list(parser.keys())])
        for coluna, details in parser.items():
            dataframe = dataframe.withColumnRenamed(coluna,
                                                    details["name"])
            dataframe = dataframe.withColumn(details["name"],
                                             F.col(details["name"]).cast(details["type"]))
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
        
        dataframe = dataframe.withColumn(colName="instituição financeira",
                                         col=regexp_replace("instituição financeira",
                                                            " \(conglomerado\)",
                                                            ""))
        dataframe = dataframe.withColumn(colName="instituição financeira",
                                         col=regexp_replace("instituição financeira",
                                                            "Ô",
                                                            "O"))
        dataframe = dataframe.withColumn(colName="Trimestre",
                                         col=regexp_replace("Trimestre",
                                                            "º",
                                                            ""))
        dataframe = dataframe.withColumn(colName="índice",
                                         col=regexp_replace("índice",
                                                            ",",
                                                            "\."))
        dataframe = self._parse_columns(dataframe)
        dataframe = dataframe.withColumn("dat_hor_psst",
                                         F.current_timestamp())
        return dataframe

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
        path: str = f"s3://pecepoli-usp-sot-458982960441/{self.output_table.split('.')[1]}/"
        df_final.write\
                .mode("overwrite")\
                .partitionBy(["ano", "num_trimestre"])\
                .parquet(path=path,
                         compression="snappy")
        # Operação MSCK é custosa, em um ambiente real o
        # ideal é utilizar a SDK da AWS boto3 para adicionar
        # as partições diretamente no Glue Data Catalog
        self.spark.sql("MSCK REPAIR TABLE db_sot.tb_reclamacoes")

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
                               'INPUT_DATABASE',
                               'INPUT_TABLE',
                               'OUTPUT_DATABASE',
                               'OUTPUT_TABLE'])
    ETL: ETLJob = ETLJob(input_table=f'{args["INPUT_DATABASE"]}.{args["INPUT_TABLE"]}',
                         output_table=f'{args["OUTPUT_DATABASE"]}.{args["OUTPUT_TABLE"]}')
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()
