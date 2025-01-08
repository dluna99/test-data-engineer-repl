from google.cloud import bigquery

def carga_linea_aerea_final ( dataset_origen, tabla_origen, dataset_destino, tabla_destino ):

    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the SQL for the stored procedure
    sql = f"""
    MERGE `{dataset_destino}.{tabla_destino}` AS linea
    USING `{dataset_origen}.{tabla_origen}`  AS linea_temp
    ON linea.code = linea_temp.code
    WHEN MATCHED THEN
    UPDATE SET 
        linea.linea_aerea = linea_temp.linea_aerea, 
        linea.fecha_actualizacion = CURRENT_DATE
    WHEN NOT MATCHED THEN
    INSERT (
        code, 
        linea_aerea, 
        fecha_creacion, 
        fecha_actualizacion
    ) VALUES (
        linea_temp.code, 
        linea_temp.linea_aerea, 
        CURRENT_DATE,
        CURRENT_DATE
    ) """ 

    #print("---------------------- \n")
    #print( sql )
    #print("---------------------- \n")
    # Execute the SQL command 
    try:
        client.query(sql).result()  # Executes the query and waits for it to finish
        print(f"Tabla `{tabla_destino}` actualizada exitosamente en `{dataset_destino}`.")
        return f"{dataset_destino}.{tabla_destino}"
    except Exception as e:
        print(f"Error al hacer el merge de la tabla: {e}")