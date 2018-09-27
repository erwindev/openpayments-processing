from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re

spark = SparkSession.builder.getOrCreate()


def read_npi_data():
    df = spark.read.csv('data/NPPES/npidata_pfile_20050523-20180812.csv', inferSchema = True, header = True)
    return df

def process_npi_data(df):

    new_column_name_list= list(map(lambda x: convert_string_to_sql_column(x), df.columns))

    df = df.toDF(*new_column_name_list)

    df.write.format('jdbc').options(
              url='jdbc:postgresql://localhost:5432/erwindb',
              driver='org.postgresql.Driver',
              batchsize=1000
              dbtable='npi_spark',
              user='erwin',
              password='my-very-secure-password').mode('append').save()

def convert_string_to_sql_column(column_name):
    valid_column_name = re.sub('[^0-9a-zA-Z]+', '_', column_name).lower()
    if valid_column_name.endswith('_'):
        valid_column_name = valid_column_name[:-1]
    return valid_column_name

if __name__ == '__main__':
    process_npi_data(read_npi_data())
