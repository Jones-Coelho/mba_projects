import os
import re
import polars as pl


def read_csv_files_recursively(root_folder: str,
                               separator: str = ";",
                               has_header: bool = False,
                               options: dict = {}) -> pl.DataFrame:
    dataframes = []
    
    # Walk through each subfolder and read .csv files
    for subdir, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                df = pl.read_csv(file_path,
                                 separator=separator,
                                 has_header=has_header,
                                 infer_schema_length=10000,
                                 **options)
                dataframes.append(df)
    
    # Concatenate the dataframes into a single dataframe
    concatenated_df = pl.concat(dataframes)
    
    return concatenated_df

def list_files_directory(directory_path):
    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory.")
        return
    files_to_return: list = []
    print(f"Listing files with size > 0 bytes in {directory_path}:")
    
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            
            if file_size > 0:
                files_to_return.append(file_path)
    return files_to_return


def split_csv_file(data: list,
                   sep: str = ","):
    table: list = [row.replace("\n","").split(sep) for row in data]
    return table


def apply_regexp_replace(data: list,
                         col_name: str,
                         pattern: str,
                         replace_string: str,
                         column_position: dict) -> list:
    table: list = data.copy()
    has_header: bool = max([col in column_position.keys() for col in data[0]])
    begin_index: int = 0
    if has_header:
        begin_index = 1
    for row in range(len(table))[begin_index:]:
        matches: list = re.findall(string=table[row][column_position[col_name]],
                                   pattern=pattern)
        for match in matches:
            table[row][column_position[col_name]] = table[row][column_position[col_name]].replace(match, replace_string)
    return table


def apply_regexp_extract(data: list,
                         col_name: str,
                         pattern: str,
                         index_group: int,
                         column_position: dict) -> list:
    table: list = data.copy()
    has_header: bool = max([col in column_position.keys() for col in data[0]])
    begin_index: int = 0
    if has_header:
        begin_index = 1
    for row in range(len(table))[begin_index:]:
        matches: list = re.findall(string=table[row][column_position[col_name]],
                                   pattern=pattern)
        if len(matches):
            table[row][column_position[col_name]] = matches[index_group]
    return table


def read_csv(path: str,
             header: bool = False,
             sep: str = ",",
             encoding: str = "UTF-8") -> list:
    files: list = list_files_directory(directory_path=path)
    files.sort(reverse=True)
    file_data: list = []
    for file_path in files:
        with open(file=file_path,
                  mode="r",
                  encoding=encoding) as file:
            data: list = file.readlines()
            data = split_csv_file(data=data,
                                  sep=sep)
        if data[0] not in file_data[:1] and header:
            file_data.append(data[0])
        if header:
            file_data = [*file_data,
                         *data[1:]]
    return file_data


def convert_data(data: list,
                 col_name: str,
                 data_type: str,
                 column_position: dict) -> list:
    convert_from_string_dict: dict = {
        "int": lambda x: int(float(x.strip())) if x.strip() != '' else "",
        "float": lambda x: float(x.strip()) if x.strip() != '' else "",
        "str": lambda x: x
    }
    table: list = data.copy()
    for row in table[1:]:
        if type(row[column_position[col_name]]) == str:
            row[column_position[col_name]] = convert_from_string_dict[data_type](row[column_position[col_name]])
    return table


def apply_select(data: list,
                 colunas: list,
                 columns_position: dict) -> list:
    table: list = data.copy()
    new_table: list = []
    for row in table:
        default_row: list = [None] * len(colunas)
        for col in range(len(colunas)):
            default_row[col] = row[columns_position[colunas[col]]]
        new_table.append(default_row)
    return new_table
