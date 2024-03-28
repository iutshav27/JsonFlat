#Importing libraries:
import sys
from pyspark.sql.transforms import *
from aws.glue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# starting sparksession 

spark = SparkSession.builder.appname('JsonFlatneer').getOrcreate()

# defining the functions for parsing and flatning  where nested structure could be 
# coulumn: arrays and column : struct

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        #Array
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn('column_name', explode(column_name))
            column_list.append(column_name)
        
        #struct
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + '.' + field.name ).alias(column_name + '_' + field.name))
        
        else:
            column_list.append(column_name)

    df = df.select(column_list)
    return df

def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True;
    return df;

def main():
    #get params from the lambda handler 
    args = getResolvedOptions(sys.argv, ["Val1", "Val2"])

    bucket_name = args["Val1"]
    file_name = args["Val2"]

    input_file_path = 's3a:/{}/{}'.format(bucket_name, file_name)

    df= spark.read.options(multiline = "True",inferSchema = 'False' ).json('input_file_path')
    df1 = flatten(df)
    df1.coalesce(1).write.mode("overwrite").options(header = 'True').csv('s3a://destinationbucket/{}'.format(file_name.split('.'),[0]))
    print('Json flattned and coverted to csv')

    print





            

        



