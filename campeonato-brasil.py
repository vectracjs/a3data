import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.types import StructField,IntegerType, StructType,StringType
from pyspark.sql.functions import *


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def run_glue_crawler(database_name, table_name, s3_path):
    # Initialize a Boto3 Glue client
    glue_client = boto3.client('glue')

    # Create a Glue Crawler Name
    crawler_name = f'{database_name}_{table_name}_crawler'


    try:
        # Delete the Glue Crawler
        glue_client.delete_crawler(Name=crawler_name)
        print(f"Glue Crawler '{crawler_name}' has been deleted.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Glue Crawler '{crawler_name}' not found.")
    except Exception as e:
        print(f"An error occurred while deleting the Glue Crawler: {str(e)}")


    # Define the Glue Crawler configuration
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::850202893763:role/glue-role_full',  # Replace with your Glue IAM Role ARN
        'DatabaseName': database_name,
        'Targets': {
            'S3Targets': [{'Path': s3_path}]
        }
    }

    # Create the Glue Crawler
    response = glue_client.create_crawler(**crawler_config)

    # Start the Glue Crawler
    response = glue_client.start_crawler(Name=crawler_name)

    return f"Glue Crawler '{crawler_name}' has been created and started."

def main():
    
    # caminho dos tabelas a serem carregadas no workload landing zone
    path1 = 's3://teste-camp-brasil/raw/campeonato-brasileiro-cartoes.csv'
    path2 = 's3://teste-camp-brasil/raw/campeonato-brasileiro-estatisticas-full.csv'
    path3 = 's3://teste-camp-brasil/raw/campeonato-brasileiro-full.csv'
    path4 = 's3://teste-camp-brasil/raw/campeonato-brasileiro-gols.csv'


    df_cartoes      = spark.read.option("delimiter", ",").option("header", True).csv(path1)
    df_estat_full   = spark.read.option("delimiter", ",").option("header", True).csv(path2)
    df_full         = spark.read.option("delimiter", ",").option("header", True).csv(path3)
    df_gols         = spark.read.option("delimiter", ",").option("header", True).csv(path4)

    # ajuste de datatypes
    df_cartoes = df_cartoes.withColumn("partida_id",df_cartoes.partida_id.cast(IntegerType()))\
        .withColumn("rodada",    df_cartoes.rodada.cast(IntegerType()))
    
    df_estat_full = df_estat_full.withColumn("partida_id",df_estat_full.partida_id.cast(IntegerType()))\
        .withColumn("rodada",    df_estat_full.rodada.cast(IntegerType()))\
        .withColumn("faltas",    df_estat_full.faltas.cast(IntegerType()))\
        .withColumn("cartao_amarelo",  df_estat_full.cartao_amarelo.cast(IntegerType()))\
        .withColumn("cartao_vermelho", df_estat_full.cartao_vermelho.cast(IntegerType()))\
        .withColumn("impedimentos",    df_estat_full.impedimentos.cast(IntegerType()))\
        .withColumn("escanteios",      df_estat_full.escanteios.cast(IntegerType()))
    
    df_full = df_full.withColumn("ID",df_full.ID.cast(IntegerType()))\
        .withColumn("rodada",df_full.rodada.cast(IntegerType()))\
        .withColumn("data", to_date("data","yyyy/MM/dd"))\
        .withColumn("mandante_placar",df_full.mandante_placar.cast(IntegerType()))\
        .withColumn("visitante_placar",df_full.visitante_placar.cast(IntegerType())) 
    
    df_gols = df_gols.withColumn("partida_id",df_gols.partida_id.cast(IntegerType()))\
        .withColumn("rodada",    df_gols.rodada.cast(IntegerType()))

    print(f'Cartões:{df_cartoes.count()}\n')
    print(f'Estat_fulls:{df_estat_full.count()}\n')
    print(f'Full:{df_full.count()}\n')
    print(f'Gols:{df_cartoes.count()}\n')
    
    #gravando tabelas no s3
    df_cartoes.coalesce(1).write\
    .format("parquet")\
    .mode("overwrite")\
    .save("s3://teste-camp-brasil/gold/cartoes/")
    
    df_estat_full.coalesce(1).write\
    .format("parquet")\
    .mode("overwrite")\
    .save("s3://teste-camp-brasil/gold/estat_full/")
    
    df_full.coalesce(1).write\
    .format("parquet")\
    .mode("overwrite")\
    .save("s3://teste-camp-brasil/gold/brasileiro_full/")
    
    df_gols.coalesce(1).write\
    .format("parquet")\
    .mode("overwrite")\
    .save("s3://teste-camp-brasil/gold/gols/")
    

    
    database_name = 'gerson'
 
    # crawler cartoes
    table_name = 'cartoes'
    s3_path = 's3://teste-camp-brasil/gold/cartoes/'
    result = run_glue_crawler(database_name, table_name, s3_path)
    print(result)

    # crawler estat_full
    table_name = 'estatisticas_full'
    s3_path = 's3://teste-camp-brasil/gold/estat_full/'
    result = run_glue_crawler(database_name, table_name, s3_path)
    print(result)


    # crawler brasileiro_full
    table_name = 'brasileiro_full'
    s3_path = 's3://teste-camp-brasil/gold/brasileiro_full/'
    result = run_glue_crawler(database_name, table_name, s3_path)
    print(result)


    # crawler gols
    table_name = 'gols'
    s3_path = 's3://teste-camp-brasil/gold/gols/'
    result = run_glue_crawler(database_name, table_name, s3_path)
    print(result)


if __name__ == "__main__":
    main()


