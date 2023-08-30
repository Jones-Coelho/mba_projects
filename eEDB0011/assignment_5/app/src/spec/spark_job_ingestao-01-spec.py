import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pydeequ.analyzers import AnalysisRunner
from pydeequ.analyzers import Size
from pydeequ.analyzers import Completeness
from pydeequ.analyzers import AnalyzerContext
from pydeequ.checks import Check
from pydeequ.checks import CheckLevel
from pydeequ.verification import VerificationSuite
from pydeequ.verification import VerificationResult
from pydeequ.analyzers import CountDistinct
from pydeequ.analyzers import Maximum
from pydeequ.analyzers import Minimum
from pydeequ.analyzers import Mean
from pydeequ.analyzers import StandardDeviation
# Author: Jones Coelho


class ETLJob:
    """
    A class that performs ETL (Extract, Transform, Load)
    """

    def __init__(self,
                 table_bancos: str = "db_sot.tb_bancos",
                 table_empregados: str = "db_sot.tb_empregados",
                 table_reclamacoes: str = "db_sot.tb_reclamacoes",
                 output_table: str = "db_spec.tb_report",
                 bucket: str = None
                 ):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.bancos = table_bancos
        self.empregados = table_empregados
        self.reclamacoes = table_reclamacoes
        self.bucket = bucket
        
        self.output_table = output_table
       

    def extract(self) -> dict:
        """
        Reads a CSV file from a specified location and
        returns a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the file contents.
        """
        dataframes: dict = {
            "bancos": self.spark
                          .read
                          .table(self.bancos)
                          .select("nom_segto_instituicao",
                                  "num_cnpj_instituicao",
                                  "nom_instituicao"),
            "empregados": self.spark
                              .read
                              .table(self.empregados)
                              .select("vlr_ind_satis_geral",
                                      "vlr_ind_remu_bene",
                                      "nom_instituicao"),
            "reclamacoes": self.spark
                               .read
                               .table(self.reclamacoes)
                               .select("num_cnpj_instituicao",
                                       "nom_instituicao",
                                       "qtd_totl_clie_css_scr",
                                       "qtd_totl_recl",
                                       "vlr_ind_recl",
                                       "ano",
                                       "num_trimestre")
        }
        return dataframes
    
    def _parse_columns(self, dataframe: DataFrame) -> DataFrame:
        parser: dict = {
            "nom_instituicao": {
                "type": "string",
                "name": "nome_banco"
            },
            "num_cnpj_instituicao": {
                "type": "bigint",
                "name": "cnpj"
            },
            "nom_segto_instituicao": {
                "type": "string",
                "name": "classificacao_banco"
            },
            "max_qtd_totl_clie_ccs_scr": {
                "type": "int",
                "name": "quantidade_clientes_banco"
            },
            "avg_ind_recl": {
                "type": "float",
                "name": "indice_reclamacoes"
            },
            "sum_qtd_totl_recl": {
                "type": "int",
                "name": "quantidade_reclamacoes"
            },
            "avg_ind_geral_instituicao": {
                "type": "float",
                "name": "indice_satisfacao_funcionarios_bancos"
            },
            "avg_ind_remu_bene_instituicao": {
                "type": "float",
                "name": "indice_satisfação_salarios_funcionarios_bancos"
            }}
        dataframe = dataframe.select([c for c in list(parser.keys())])
        for coluna, details in parser.items():
            dataframe = dataframe.withColumnRenamed(coluna,
                                                    details["name"])
            dataframe = dataframe.withColumn(details["name"],
                                             F.col(details["name"])
                                              .cast(details["type"]))
        return dataframe

    def transform(self, dataframes: dict) -> DataFrame:
        """
        Executes transformations on the input DataFrame based
        on business requirements.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame ready to be loaded.
        """
        dataframes["bancos"].createOrReplaceTempView("bancos")
        dataframes["empregados"].createOrReplaceTempView("empregados")
        dataframes["reclamacoes"].createOrReplaceTempView("reclamacoes")
        QUERY = """
            SELECT max(bancos.nom_segto_instituicao)
                   as nom_segto_instituicao,
                   bancos.num_cnpj_instituicao,
                   max(bancos.nom_instituicao) as nom_instituicao,
                   max(reclamacoes.qtd_totl_clie_css_scr)
                   as qtd_totl_clie_css_scr,
                   max(reclamacoes.qtd_totl_recl) as qtd_totl_recl,
                   max(reclamacoes.vlr_ind_recl) as vlr_ind_recl,
                   max(empregados.vlr_ind_remu_bene) as vlr_ind_remu_bene,
                   max(empregados.vlr_ind_satis_geral) as vlr_ind_satis_geral
            FROM bancos
            INNER JOIN reclamacoes
            ON bancos.num_cnpj_instituicao = reclamacoes.num_cnpj_instituicao
            OR bancos.nom_instituicao = reclamacoes.nom_instituicao
            INNER JOIN empregados
            ON bancos.nom_instituicao = empregados.nom_instituicao
            GROUP BY bancos.num_cnpj_instituicao
        """
        self.spark.sql(QUERY).createOrReplaceTempView("temp_report_analitico")
        QUERY = """
                    SELECT nom_instituicao,
                        num_cnpj_instituicao,
                        nom_segto_instituicao,
                        max(qtd_totl_clie_css_scr)
                        as max_qtd_totl_clie_ccs_scr,
                        avg(vlr_ind_recl) as avg_ind_recl,
                        sum(qtd_totl_recl) as sum_qtd_totl_recl,
                        avg(vlr_ind_satis_geral) as avg_ind_geral_instituicao,
                        avg(vlr_ind_remu_bene) as avg_ind_remu_bene_instituicao
                    FROM temp_report_analitico
                    GROUP BY nom_instituicao,
                            num_cnpj_instituicao,
                            nom_segto_instituicao
                """
        dataframe = self.spark.sql(QUERY)
        dataframe = self._parse_columns(dataframe)
        dataframe = dataframe.withColumn("dat_hor_psst",
                                         F.current_timestamp())
        dataframe = dataframe.withColumn("anomesdia",
                                         F.lit(datetime.now().strftime("%Y%m%d")))
        return dataframe

    def load(self,
             dataframe: DataFrame) -> None:
        """
        Saves a DataFrame to a mysql database.

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
                .partitionBy("anomesdia")\
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
        df: DataFrame = self.transform(self.extract()).persist()
        self.data_quality(df)
        self.load(df)

    def data_quality(self, dataframe: DataFrame) -> None:
        path_output_data_quality: str = "s3://pecepoli-usp-spec-458982960441/data_quality/"\
                                        f"anomesdia={datetime.now().strftime('%Y%m%d')}/"\
                                        f"nom_tab={self.output_table.split('.')[1]}/" # noqa
        analysis_result = AnalysisRunner(self.spark).onData(dataframe)\
                                                    .addAnalyzer(Size())\
                                                    .addAnalyzer(Completeness("nome_banco"))\
                                                    .addAnalyzer(Completeness("cnpj"))\
                                                    .addAnalyzer(Completeness("classificacao_banco"))\
                                                    .addAnalyzer(Maximum("quantidade_clientes_banco"))\
                                                    .addAnalyzer(Minimum("quantidade_clientes_banco"))\
                                                    .addAnalyzer(Mean("quantidade_clientes_banco"))\
                                                    .addAnalyzer(StandardDeviation("quantidade_clientes_banco"))\
                                                    .addAnalyzer(Maximum("indice_reclamacoes"))\
                                                    .addAnalyzer(Minimum("indice_reclamacoes"))\
                                                    .addAnalyzer(Mean("indice_reclamacoes"))\
                                                    .addAnalyzer(StandardDeviation("indice_reclamacoes"))\
                                                    .addAnalyzer(Maximum("quantidade_reclamacoes"))\
                                                    .addAnalyzer(Minimum("quantidade_reclamacoes"))\
                                                    .addAnalyzer(Mean("quantidade_reclamacoes"))\
                                                    .addAnalyzer(StandardDeviation("quantidade_reclamacoes"))\
                                                    .addAnalyzer(Maximum("indice_satisfacao_funcionarios_bancos"))\
                                                    .addAnalyzer(Minimum("indice_satisfacao_funcionarios_bancos"))\
                                                    .addAnalyzer(Mean("indice_satisfacao_funcionarios_bancos"))\
                                                    .addAnalyzer(StandardDeviation("indice_satisfacao_funcionarios_bancos"))\
                                                    .addAnalyzer(Maximum("indice_satisfação_salarios_funcionarios_bancos"))\
                                                    .addAnalyzer(Minimum("indice_satisfação_salarios_funcionarios_bancos"))\
                                                    .addAnalyzer(Mean("indice_satisfação_salarios_funcionarios_bancos"))\
                                                    .addAnalyzer(StandardDeviation("indice_satisfação_salarios_funcionarios_bancos"))\
                                                    .addAnalyzer(StandardDeviation("vlr_ind_remu_bene"))\
                                                    .addAnalyzer(CountDistinct("cnpj"))\
                                                    .run() # noqa
        analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, # noqa
                                                                       analysis_result) # noqa
        # add timestamp for data quality
        analysis_result_df = analysis_result_df.withColumn("dat_hor_psst",
                                                           F.current_timestamp()) # noqa
        analysis_result_df.write\
                          .mode("append")\
                          .parquet(path=path_output_data_quality,
                                   compression="snappy")
        self.spark.sql("MSCK REPAIR TABLE db_spec.data_quality")
        check = Check(self.spark, CheckLevel.Warning, "Review Check")
        check_result = VerificationSuite(self.spark).onData(dataframe)\
                                                    .addCheck(check.hasSize(lambda size: size >= 24)\
                                                    .hasCompleteness("nom_instituicao", lambda completeness: completeness == 1.0)\
                                                    .hasCompleteness("cnpj", lambda completeness: completeness == 1.0)\
                                                    .hasCompleteness("classificacao_banco", lambda completeness: completeness == 1.0)\
                                                    .isNonNegative("quantidade_clientes_banco")\
                                                    .isNonNegative("indice_reclamacoes")\
                                                    .isNonNegative("quantidade_reclamacoes")\
                                                    .isNonNegative("indice_satisfacao_funcionarios_bancos")\
                                                    .isNonNegative("indice_satisfação_salarios_funcionarios_bancos"))\
                                                    .run() # noqa
        check_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, # noqa
                                                                     check_result) # noqa
        if check_result_df.filter(F.col("constraint_status") != "Success").count():
            check_result_df.show()
            raise Exception("Data Quality Check Failed")


if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                              ["JOB_NAME",
                               "TABLE_BANCOS",
                               "TABLE_EMPREGADOS",
                               "TABLE_RECLAMACOES",
                               "OUTPUT_TABLE",
                               "BUCKET"])
    ETL: ETLJob = ETLJob(table_bancos=args.get("TABLE_BANCOS"),
                         table_empregados=args.get("TABLE_EMPREGADOS"),
                         table_reclamacoes=args.get("TABLE_RECLAMACOES"),
                         output_table=args.get("OUTPUT_TABLE"),
                         bucket=args.get("BUCKET")
                         )
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()
