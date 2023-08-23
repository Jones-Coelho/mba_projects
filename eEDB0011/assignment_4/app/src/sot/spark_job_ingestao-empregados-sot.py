import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_extract
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
            "reviews_count": {
                "type": "int",
                "name": "qtd_aval"
            },
            "culture_count": {
                "type": "int",
                "name": "qtd_aval_cult"
            },
            "salaries_count": {
                "type": "int",
                "name": "qtd_aval_salr"
            },
            "benefits_count": {
                "type": "int",
                "name": "qtd_aval_benef"
            },
            "employer-website": {
                "type": "string",
                "name": "txt_url_instituicao"
            },
            "employer-headquarters-city": {
                "type": "string",
                "name": "nom_mun_sede_instituicao"
            },
            "employer-headquarters-country": {
                "type": "string",
                "name": "nom_pais_sede_instituicao"
            },
            "employer-founded": {
                "type": "int",
                "name": "ano_fund_instituicao"
            },
            "employer-industry": {
                "type": "string",
                "name": "nom_segto_atua_instituicao"
            },
            "employer-revenue": {
                "type": "string",
                "name": "txt_faixa_rend_instituicao"
            },
            "url": {
                "type": "string",
                "name": "txt_url_orig_data"
            },
            "geral": {
                "type": "float",
                "name": "vlr_ind_satis_geral"
            },
            "cultura e valores": {
                "type": "float",
                "name": "vlr_ind_cult_valrs"
            },
            "diversidade e inclusão": {
                "type": "float",
                "name": "vlr_ind_divr_incl"
            },
            "qualidade de vida": {
                "type": "float",
                "name": "vlr_ind_qual_vida"
            },
            "alta liderança": {
                "type": "float",
                "name": "vlr_ind_alt_lider"
            },
            "remuneração e benefícios": {
                "type": "float",
                "name": "vlr_ind_remu_bene"
            },
            "oportunidades de carreira": {
                "type": "float",
                "name": "vlr_ind_oprt_carr"
            },
            "recomendam para outras pessoas(%)": {
                "type": "float",
                "name": "vlr_pct_recom_outr_pess"
            },
            "perspectiva positiva da empresa(%)": {
                "type": "float",
                "name": "vlr_pct_perspec_pos_instituicao"
            },
            "nome": {
                "type": "string",
                "name": "nom_instituicao"
            }}
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
        dataframe = dataframe.filter(F.substring(F.col("segmento"),1,1) == 'S')
        dataframe = dataframe.withColumn("employer-headquarters-city",
                                        F.element_at(F.split(F.col("employer-headquarters"),", "), 1))
        dataframe = dataframe.withColumn("employer-headquarters-country",
                                        F.element_at(F.split(F.col("employer-headquarters"),", "), 2))
        dataframe = dataframe.withColumn(colName="employer-industry",
                                         col=regexp_extract("employer-industry",
                                                            "\/.+\/(.+)\..,",
                                                            1))
        dataframe = self._parse_columns(dataframe)
        dataframe = dataframe.withColumn("dat_hor_psst",
                                         F.current_timestamp())
        dataframe = dataframe.withColumn("anomesdia",
                                         F.lit(datetime.now().strftime("%Y%m%d")))
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
                .partitionBy("anomesdia")\
                .parquet(path=path,
                         compression="snappy")
        # Operação MSCK é custosa, em um ambiente real o
        # ideal é utilizar a SDK da AWS boto3 para adicionar
        # as partições diretamente no Glue Data Catalog
        self.spark.sql("MSCK REPAIR TABLE db_sot.tb_empregados")

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