from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_nested_json(df):
    column_list = []

    for column_name in df.schema.names:
        print("Outside isinstance loop: " + column_name)
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)

        elif isinstance(df.schema[column_name].dataType, StructType):
            print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)

    # Selecting columns using column_list from dataframe: df
    df = df.select(column_list)
    return df

read_nested_json_flag = True

while read_nested_json_flag:
  print("Reading Nested JSON File untill normailization ")
  df = read_nested_json(df=df)
  read_nested_json_flag = False
  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True
