from app.utils import utils
from datetime import datetime
from datetime import date
import os


class ETLJob:

    def __init__(self,
                 input_path: str,
                 output_path: str,
                 dat_ref: date):
        self.input_path: str = input_path
        self.ouput_path: str = output_path
        self.ref_date: date = dat_ref
    
    def _set_schema(self, header: list):
        self.schema: dict = {}
        for col in range(len(header)):
            if header[col].replace('"', '') not in self.schema.keys():
                self.schema[header[col].replace('"', '')] = col

    def _parse_columns(self, data: list) -> list:
        table: list = data.copy()
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
                "type": "str",
                "name": "txt_url_instituicao"
            },
            "employer-headquarters-city": {
                "type": "str",
                "name": "nom_mun_sede_instituicao"
            },
            "employer-headquarters-country": {
                "type": "str",
                "name": "nom_pais_sede_instituicao"
            },
            "employer-founded": {
                "type": "int",
                "name": "ano_fund_instituicao"
            },
            "employer-industry": {
                "type": "str",
                "name": "nom_segto_atua_instituicao"
            },
            "employer-revenue": {
                "type": "str",
                "name": "txt_faixa_rend_instituicao"
            },
            "url": {
                "type": "str",
                "name": "txt_url_orig_data"
            },
            "Geral": {
                "type": "float",
                "name": "vlr_ind_satis_geral"
            },
            "Cultura e valores": {
                "type": "float",
                "name": "vlr_ind_cult_valrs"
            },
            "Diversidade e inclusão": {
                "type": "float",
                "name": "vlr_ind_divr_incl"
            },
            "Qualidade de vida": {
                "type": "float",
                "name": "vlr_ind_qual_vida"
            },
            "Alta liderança": {
                "type": "float",
                "name": "vlr_ind_alt_lider"
            },
            "Remuneração e benefícios": {
                "type": "float",
                "name": "vlr_ind_remu_bene"
            },
            "Oportunidades de carreira": {
                "type": "float",
                "name": "vlr_ind_oprt_carr"
            },
            "Recomendam para outras pessoas(%)": {
                "type": "float",
                "name": "vlr_pct_recom_outr_pess"
            },
            "Perspectiva positiva da empresa(%)": {
                "type": "float",
                "name": "vlr_pct_perspec_pos_instituicao"
            },
            "Nome": {
                "type": "str",
                "name": "nom_instituicao"
            }}

        for coluna, details in parser.items():
            table[0][self.schema[coluna]] = details["name"]
            self.schema[details["name"]] = self.schema[coluna]
            self.schema.pop(coluna)
            table = utils.convert_data(data=table,
                                       col_name=details["name"],
                                       data_type=details["type"],
                                       column_position=self.schema)
        table = utils.apply_select(data=table,
                                   colunas=[parser[c]["name"]
                                            for c
                                            in list(parser.keys())],
                                   columns_position=self.schema)
        return table
        
            
    def extract(self) -> list:
        raw_data: list = utils.read_csv(path=self.input_path,
                                        header=True,
                                        sep=";")
        self._set_schema(raw_data[0])
        return raw_data
    
    def transform(self, data: list) -> list:
        table: list = data.copy()
        table = [table[0],
                 *[row
                  for row
                  in table[1:]
                  if row[self.schema["Segmento"]][0] == "S"]]
        table[0].append('"employer-headquarters-city"')
        self.schema["employer-headquarters-city"] = table[0].index('"employer-headquarters-city"')
        for row in range(1, len(table)):
            table[row].append(table[row][self.schema["employer-headquarters"]].split(", ")[0])

        table[0].append('"employer-headquarters-country"')
        self.schema["employer-headquarters-country"] = table[0].index('"employer-headquarters-country"')
        for row in range(1, len(table)):
            table[row].append(table[row][self.schema["employer-headquarters"]].split(", ")[1])
    
        table = utils.apply_regexp_extract(data=table,
                                           col_name="employer-industry",
                                           pattern="\/.+\/(.+)\..,",
                                           index_group=0,
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Nome",
                                           pattern=" - PRUDENCIAL",
                                           replace_string="",
                                           column_position=self.schema)
        table = self._parse_columns(data=table)
        table[0].append('"dat_hor_psst"')
        self.schema["dat_hor_psst"] = len(table[0]) - 1
        table[0].append('"anomesdia"')
        self.schema["anomesdia"] = len(table[0]) - 1
        for row in range(1, len(table)):
            table[row].append(datetime.timestamp(datetime.now()))
            table[row].append(self.ref_date.strftime("%Y%m%d"))
        return table


    def load(self, data: list) -> None:
        table_to_load: list = data.copy()
        header: list = table_to_load[0]
        header: str = ";".join([f'"{i}"'
                                for i
                                in header[:-1]]) + "\n"
        particoes: dict = {}
        for row in table_to_load[1:]:
            if row[-1] not in particoes.keys():
                particoes[row[-1]] = []
            particoes[row[-1]].append(row[:-1])
        for particao in particoes.keys():
            data_to_write: list = [";".join([f'"{i}"' if type(i) == "str" else f'{i}'
                                             for i
                                             in row]) + "\n"
                                    for row
                                    in particoes[particao]]
            folder_output: str =f"{self.ouput_path}/anomesdia={particao}/"
            if not os.path.isdir(folder_output):
                os.makedirs(folder_output, exist_ok=True)
            with open(f"{folder_output}tb_bancos.csv",
                      "w+",
                      encoding="UTF-8") as file:
                file.writelines([*header, *data_to_write])
    def run(self) -> None:
        """
        Executes the complete ETL process.

        Args:
            None

        Returns:
            None
        """
        self.load(self.transform(self.extract()))
