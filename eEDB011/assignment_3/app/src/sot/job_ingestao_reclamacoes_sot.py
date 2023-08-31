from app.utils import utils
from datetime import datetime
import os


class ETLJob:

    def __init__(self,
                 input_path: str,
                 output_path: str):
        self.input_path: str = input_path
        self.ouput_path: str = output_path
    
    def _set_schema(self, header: list):
        self.schema: dict = {}
        for col in range(len(header)):
            if header[col].replace('"', '') not in self.schema.keys():
                self.schema[header[col].replace('"', '')] = col

    def _parse_columns(self, data: list) -> list:
        table: list = data.copy()
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
                "type": "str",
                "name": "nom_categoria"
            },
            "Tipo": {
                "type": "str",
                "name": "nom_tipo_instituicao"
            },
            "CNPJ IF": {
                "type": "int",
                "name": "num_cnpj_instituicao"
            },
            "Instituição financeira": {
                "type": "str",
                "name": "nom_instituicao"
            },
            "Índice": {
                "type": "float",
                "name": "vlr_ind_recl"
            },
            "Quantidade de reclamações reguladas procedentes": {
                "type": "int",
                "name": "qtd_recl_reg_prec"
            },
            "Quantidade de reclamações reguladas - outras": {
                "type": "int",
                "name": "qtd_recl_reg_outr"
            },
            "Quantidade de reclamações não reguladas": {
                "type": "int",
                "name": "qtd_recl_nao_reg"
            },
            "Quantidade total de reclamações": {
                "type": "int",
                "name": "qtd_totl_recl"
            },
            "Quantidade total de clientes  CCS e SCR": {
                "type": "int",
                "name": "qtd_totl_clie_css_scr"
            },
            "Quantidade de clientes  CCS": {
                "type": "int",
                "name": "qtd_clie_ccs"
            },
            "Quantidade de clientes  SCR": {
                "type": "int",
                "name": "qtd_clie_scr"
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
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Instituição financeira",
                                           pattern=" \(conglomerado\)",
                                           replace_string="",
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Instituição financeira",
                                           pattern="Ô",
                                           replace_string="O",
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Trimestre",
                                           pattern="º",
                                           replace_string="",
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Índice",
                                           pattern="\.",
                                           replace_string="#",
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Índice",
                                           pattern=",",
                                           replace_string=".",
                                           column_position=self.schema)
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Índice",
                                           pattern="#",
                                           replace_string="",
                                           column_position=self.schema)
        table = self._parse_columns(data=table)
        
        table[0].append('dat_hor_psst')
        self.schema["dat_hor_psst"] = len(table[0]) - 1
        table[0].append('anomesdia')
        self.schema["anomesdia"] = len(table[0]) - 1
        for row in range(1, len(table)):
            table[row].append(datetime.timestamp(datetime.now()))
            trimestre: dict = {
                "1": "03",
                "2": "06",
                "3": "09",
                "4": "12"
            }
            mes: str = trimestre.get(str(table[row][1]), "00")
            value: str = f"{table[row][0]}{mes}01"
            table[row].append(value)
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
            with open(f"{folder_output}tb_reclamacoes.csv",
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
