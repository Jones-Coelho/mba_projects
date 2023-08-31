from app.utils import utils
import os
from datetime import date

class ETLJob:

    def __init__(self,
                 input_path: str,
                 output_path: str,
                 dat_ref: date):
        self.input_path: str = input_path
        self.ouput_path: str = output_path
        self.ref_date: date = dat_ref

    def extract(self) -> list:
        raw_data: list = utils.read_csv(path=self.input_path,
                                        header=True,
                                        sep="\t")
        return raw_data
    
    def transform(self, data: list) -> list:
        table: list = data.copy()
        table[0].append("anomesdia")
        for row in table[1:]:
            date: str = self.ref_date.strftime("%Y%m%d")
            row.append(date)
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
            data_to_write: list = [";".join([f'{i}'
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
