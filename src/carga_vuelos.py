from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql.functions import monotonically_increasing_id

# Create a Spark session
spark = SparkSession.builder \
    .appName("LoadCSVToBigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
    .getOrCreate()

def cargar_CSV( folder ):
    # Load the CSV file into a DataFrame
    csv_file_path = folder
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Return dataframe from CSV file
    return df

def extraer( folder ):
    df = cargar_CSV()
    return df

def transformar( datos, sucursal ):
    df = datos

    # Check that column Sucursal is present in the dataframe
    mi_columna_sucursal = 'Sucursal'

    if mi_columna_sucursal in df.columns: 
        print(f"La columna '{mi_columna_sucursal}' está presente en el dataframe.")
    else:
        #Se agrega la columna Sucursal 
        df = df.withColumn( f'{mi_columna_sucursal}', sucursal )

    # Add a row number using monotonically_increasing_id
    mi_df_with_row_number = df.withColumn("Id", monotonically_increasing_id())
    return mi_df_with_row_number

from google.cloud import bigquery

def cargar( dataset, tabla, datos ):

    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the BigQuery dataset and table
    dataset_id = dataset  # Replace with your dataset ID
    table_id = tabla  # Replace with your table ID

    # Define the BigQuery table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    #Setup overwrite table
    job_config = bigquery.LoadJobConfig( write_disposition="WRITE_TRUNCATE")

    # Load DataFrame to BigQuery
    job = client.load_table_from_dataframe(datos, table_ref, job_config=job_config )

    # Wait for the job to complete
    job.result()

    # Confirm that the data has been loaded
    print(f"Se cargaron {job.output_rows} registros en la tabla {dataset_id}:{table_id}.")

def cargar_vuelos( dataset, tabla, folder, sucursal ):
    #mi_folder = '../data/central/CatLineasAereas.csv'
    mi_datos_originales = extraer( folder  )

    mi_datos_transformados = transformar( mi_datos_originales, sucursal )

    cargar( dataset, tabla, mi_datos_transformados )


    
