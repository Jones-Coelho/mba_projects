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

    def __init__(self, input_table, output_table, bucket):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.input_table: str = input_table
        self.output_table: str = output_table
        self.bucket: str = bucket

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
            "Ano": {
                "type": "int",
                "name": "ano"
            },
            "Trimestre": {
                "type": "int",
                "name": "num_trimestre"
            },
            "Categoria": {
                "type": "string",
                "name": "nom_categoria"
            },
            "Tipo": {
                "type": "string",
                "name": "nom_tipo_instituicao"
            },
            "CNPJ IF": {
                "type": "bigint",
                "name": "num_cnpj_instituicao"
            },
            "Instituição Financeira": {
                "type": "string",
                "name": "nom_instituicao"
            },
            "Índice": {
                "type": "float",
                "name": "vlr_ind_recl"
            },
            "Quantidade de reclamações reguladas procedentes": {
                "type": "bigint",
                "name": "qtd_recl_reg_prec"
            },
            "Quantidade de reclamações reguladas - outras": {
                "type": "bigint",
                "name": "qtd_recl_reg_outr"
            },
            "Quantidade de reclamações não reguladas": {
                "type": "bigint",
                "name": "qtd_recl_nao_reg"
            },
            "Quantidade total de reclamações": {
                "type": "bigint",
                "name": "qtd_totl_recl"
            },
            "Quantidade total de clientes  CCS e SCR": {
                "type": "bigint",
                "name": "qtd_totl_clie_css_scr"
            },
            "Quantidade de clientes  CCS": {
                "type": "bigint",
                "name": "qtd_clie_ccs"
            },
            "Quantidade de clientes  SCR": {
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
        
        dataframe = dataframe.withColumn(colName="Instituição Financeira",
                                         col=regexp_replace("Instituição Financeira",
                                                            " \(conglomerado\)",
                                                            ""))
        dataframe = dataframe.withColumn(colName="Instituição Financeira",
                                         col=regexp_replace("Instituição Financeira",
                                                            "Ô",
                                                            "O"))
        dataframe = dataframe.withColumn(colName="Trimestre",
                                         col=regexp_replace("Trimestre",
                                                            "º",
                                                            ""))
        dataframe = dataframe.withColumn(colName="Índice",
                                         col=regexp_replace("Índice",
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
        path: str = f"s3://{self.bucket}/{self.output_table.split('.')[0]}/{self.output_table.split('.')[1]}/"
        df_final.write\
                .mode("overwrite")\
                .partitionBy(["ano", "num_trimestre"])\
                .parquet(path=path,
                         compression="snappy")

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
                           'BUCKET',
                           'OUTPUT_DATABASE',
                           'OUTPUT_TABLE'])
    ETL: ETLJob = ETLJob(input_table=f'{args["INPUT_DATABASE"]}.{args["INPUT_TABLE"]}',
                         output_table=f'{args["OUTPUT_DATABASE"]}.{args["OUTPUT_TABLE"]}',
                         bucket=args["BUCKET"])
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()