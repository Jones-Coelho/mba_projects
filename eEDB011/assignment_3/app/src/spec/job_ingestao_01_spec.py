import polars as pl
from app.utils import utils
from datetime import datetime
import os
from datetime import date

class ETLJob:
    def __init__(self,
                 dat_ref: date):
        self.input_bancos: str = "./app/data/output/sot/tb_bancos/"
        self.input_empregados: str = "./app/data/output/sot/tb_empregados/"
        self.input_reclamacoes: str = "./app/data/output/sot/tb_reclamacoes/"
        self.path_output: str = "./app/data/output/spec/report_atividade/"
        self.ref_dat: date = dat_ref
        
    def extract(self) -> dict:
        dataframes = {
            "bancos": utils.read_csv_files_recursively(self.input_bancos,
                                                       separator=";",
                                                       has_header=True),
            "empregados": utils.read_csv_files_recursively(self.input_empregados,
                                                           separator=";",
                                                           has_header=True),
            "reclamacoes": utils.read_csv_files_recursively(self.input_reclamacoes,
                                                            separator=";",
                                                            has_header=True)
        }
        return dataframes

    def transform(self, dataframes: dict) -> pl.DataFrame:
        bancos = dataframes["bancos"].select([
            "nom_segto_instituicao",
            "num_cnpj_instituicao",
            "nom_instituicao"
        ])
        empregados = dataframes["empregados"].select([
            "vlr_ind_satis_geral",
            "vlr_ind_remu_bene",
            "nom_instituicao"
        ])
        reclamacoes = dataframes["reclamacoes"].select([
            "num_cnpj_instituicao",
            "nom_instituicao",
            "qtd_totl_clie_css_scr",
            "qtd_totl_recl",
            "vlr_ind_recl",
            "ano",
            "num_trimestre"
        ])

        temp_report_analitico = pl.concat([bancos.join(reclamacoes.drop("nom_instituicao"),
                                                       on="num_cnpj_instituicao",
                                                       how="inner"),
                                           bancos.join(reclamacoes.drop("num_cnpj_instituicao"),
                                                       on="nom_instituicao",
                                                       how="inner")]).unique()
        temp_report_analitico = temp_report_analitico\
            .join(empregados, on="nom_instituicao", how="inner")
        temp_report_analitico = temp_report_analitico\
            .groupby("num_cnpj_instituicao")\
            .agg([
                pl.max("nom_segto_instituicao"),
                pl.max("nom_instituicao"),
                pl.max("qtd_totl_clie_css_scr"),
                pl.max("qtd_totl_recl"),
                pl.max("vlr_ind_recl"),
                pl.max("vlr_ind_remu_bene"),
                pl.max("vlr_ind_satis_geral")
            ])
        

        dataframe = (temp_report_analitico
            .groupby(["nom_instituicao", "num_cnpj_instituicao", "nom_segto_instituicao"])
            .agg([
                pl.max("qtd_totl_clie_css_scr").alias("max_qtd_totl_clie_ccs_scr"),
                pl.mean("vlr_ind_recl").alias("avg_ind_recl"),
                pl.sum("qtd_totl_recl").alias("sum_qtd_totl_recl"),
                pl.mean("vlr_ind_satis_geral").alias("avg_ind_geral_instituicao"),
                pl.mean("vlr_ind_remu_bene").alias("avg_ind_remu_bene_instituicao")
            ])
        )

        dataframe = dataframe.with_columns(pl.lit(str(datetime.now())).alias("dat_hor_psst"))
        dataframe = dataframe.with_columns(pl.lit(self.ref_dat.strftime("%Y%m%d")).alias("anomesdia"))

        return dataframe

    def load(self, dataframe: pl.DataFrame) -> None:
        path_to_write: str = f"{self.path_output}anomesdia={self.ref_dat.strftime('%Y%m%d')}/"
        if not os.path.isdir(path_to_write):
            os.makedirs(path_to_write,
                        exist_ok=True)
        dataframe.write_csv(path_to_write+"tb_relatorio_final.csv",
                            has_header=True,
                            separator=";")

    def run(self) -> None:
        dataframes = self.extract()
        transformed_df = self.transform(dataframes)
        self.load(transformed_df)

if __name__ == '__main__':
    job = ETLJob()
    job.run()
