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
        self.schema: dict = {
            "Segmento": 0,
            "CNPJ": 1,
            "Nome": 2
        }

    def _parse_columns(self, data: list) -> list:
        table: list = data.copy()
        parser: dict = {
                    "Segmento": {
                        "type": "str",
                        "name": "nom_segto_instituicao"
                    },
                    "CNPJ": {
                        "type": "int",
                        "name": "num_cnpj_instituicao"
                    },
                    "Nome": {
                        "type": "str",
                        "name": "nom_instituicao"
                    }}
        table = utils.apply_select(data=table,
                                   colunas=[c for c in list(parser.keys())],
                                   columns_position=self.schema)
        for coluna, details in parser.items():
            table[0][self.schema[coluna]] = details["name"]
            self.schema[details["name"]] = self.schema[coluna]
            self.schema.pop(coluna)
            table = utils.convert_data(data=table,
                                       col_name=details["name"],
                                       data_type=details["type"],
                                       column_position=self.schema)
        return table
        
            
    def extract(self) -> list:
        raw_data: list = utils.read_csv(path=self.input_path,
                                        header=True,
                                        sep=";")
        return raw_data
    
    def transform(self, data: list) -> list:
        table: list = data.copy()
        table = utils.apply_regexp_replace(data=table,
                                           col_name="Nome",
                                           pattern=" - PRUDENCIAL",
                                           replace_string="",
                                           column_position=self.schema)
        table = self._parse_columns(data=table)
        table[0].append("dat_hor_psst")
        self.schema["dat_hor_psst"] = len(table[0]) - 1
        table[0].append("anomesdia")
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
