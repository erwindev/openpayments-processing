import pandas as pd
import dask.dataframe as dd
import time
from sqlalchemy import create_engine, exc
import re
import numpy as np
import math

opd_2017_file = 'data/PGYR17_P062918/OP_DTL_GNRL_PGYR2017_P06292018.csv'
nppes_npi_file = 'data/NPPES/npidata_pfile_20050523-20180812.csv'
# nppes_npi_file = 'data/NPPES/npidata_pfile_20050523-20180812-TEST.csv'
nppes_npi_header_file = 'data/NPPES/npidata_pfile_20050523-20180812_FileHeader.csv'
nppes_npi_header_subset_file = 'data/NPPES/npidata_pfile_20050523-20180812_FileHeader-SUBSET.csv'

def sample_openpayment_data():
    df = pd.read_csv(opd_2017_file, nrows=100000)
    file_split_name = opd_2017_file.split('.')
    test_file_name = file_split_name[0] + '-TEST.' + file_split_name[1]
    df.to_csv(test_file_name)
    return test_file_name

def opd_dtypes(test_opd_file_name):
    df = pd.read_csv(test_opd_file_name)
    dtypes = {k: str for k in df.columns.values}
    return dtypes

def read_openpayment_data(test_opd_file_name):
    df_openpayments = dd.read_csv(opd_2017_file, dtype=opd_dtypes(test_opd_file_name))
    return df_openpayments.compute()

def process_openpayment_data(df):
    engine = create_engine('postgresql://erwin:my-very-secure-password@localhost:5432/erwindb')
    df.to_sql(name='open_payment_data', con=engine, if_exists = 'replace', index=False)

    print(df.columns)
    columns = [
        'Physician_Profile_ID',
        'Physician_First_Name',
        'Physician_Last_Name',
        'Physician_Primary_Type',
        'Physician_Specialty',
        'Physician_License_State_code1',
        'Physician_License_State_code2',
        'Physician_License_State_code3',
        'Physician_License_State_code4',
        'Physician_License_State_code5'
    ]
    df = df[columns]
    print(len(df))

def read_npi_header():
    df = pd.read_csv(nppes_npi_header_file)
    return df

def read_npi_subset_header():
    df = pd.read_csv(nppes_npi_header_subset_file)
    return df

def npi_dtypes():
    df = read_npi_header()
    dtypes = {k: str for k in df.columns.values}
    return dtypes

def npi_column_rename():
    df = read_npi_header()
    rename_columns = {k: convert_string_to_sql_column(k) for k in df.columns.values}
    all_columns = [convert_string_to_sql_column(k) for k in df.columns.values]
    return rename_columns, all_columns

def read_npi_data():
    # df_npis = pd.read_csv(filename)
    # return df_npis

    df_npis = dd.read_csv(nppes_npi_file, dtype=npi_dtypes())
    return df_npis.compute()

def sample_npi_data():
    df = pd.read_csv(nppes_npi_file, nrows=10000)
    file_split_name = nppes_npi_file.split('.')
    test_file_name = file_split_name[0] + '-TEST.' + file_split_name[1]
    df.to_csv(test_file_name)
    return test_file_name

def process_npi_data(df):
    rename_columns, all_columns = npi_column_rename()
    df.rename(columns = rename_columns, inplace=True)
    df = df.astype(str)
    engine = create_engine('postgresql://erwin:my-very-secure-password@localhost:5432/erwindb')
    df = df.reset_index()

    # columns = [
    #     'npi',
    #     'provider_first_name',
    #     'provider_last_name_legal_name',
    #     'provider_middle_name'
    # ]

    # columns = [convert_string_to_sql_column(k) for k in read_npi_subset_header().columns.values]
    #
    # df = df[columns]

    write_df_to_sql('npi_data_ext', df, engine)
    #df.to_sql(name='npi_data', con=engine, if_exists = 'replace', index=False, chunksize=20000)

def convert_string_to_sql_column(column_name):
    valid_column_name = re.sub('[^0-9a-zA-Z]+', '_', column_name).lower()
    if valid_column_name.endswith('_'):
        valid_column_name = valid_column_name[:-1]
    return valid_column_name


def __write_df(table_name, df, engine, **kwargs):
    df.to_sql(table_name, con = engine, **kwargs)
    return True

def __write_split_df(table_name, dfs, engine, **kwargs):
    __write_df(table_name, dfs[0], engine, **kwargs)
    kwargs.pop('if_exists')
    for df in dfs[1:]:
        __write_df(table_name, df, engine, if_exists = 'append', **kwargs)
    return True

def __split_df(df, chunksize):
    chunk_count = int(math.ceil(df.size / chunksize))
    return np.array_split(df, chunk_count)

def write_df_to_sql(table_name, df, engine, if_exists = 'replace', chunksize = 10**5, **kwargs):
    s = time.time()
    status = False
    if chunksize is not None and df.size > chunksize:
        dfs = __split_df(df, chunksize)
        status = __write_split_df(table_name, dfs, engine, if_exists = if_exists,  **kwargs)
    else:
        status = __write_df(table_name, df, engine, if_exists = 'replace', **kwargs)

if __name__ == '__main__':
    # test_opd_file_name = sample_openpayment_data()
    # process_openpayment_data(read_openpayment_data(test_opd_file_name))

    process_npi_data(read_npi_data())
