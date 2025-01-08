from google.cloud import bigquery

def carga_pasajero_final ( dataset_origen, tabla_origen, dataset_destino, tabla_destino ):

    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the SQL for the stored procedure
    sql = f"""
    MERGE `{dataset_destino}.{tabla_destino}` AS pasajero
    USING `{dataset_origen}.{tabla_origen}`  AS pasajero_temp
    ON pasajero.ID_pasajero = pasajero_temp.ID_Pasajero
    WHEN MATCHED THEN
    UPDATE SET 
        pasajero.nombre_pasajero = pasajero_temp.pasajero, 
        pasajero.edad = pasajero_temp.edad, 
        pasajero.fecha_actualizacion = CURRENT_DATE
    WHEN NOT MATCHED THEN
    INSERT (
        id_pasajero, 
        nombre_pasajero, 
        edad,
        fecha_creacion, 
        fecha_actualizacion
    ) VALUES (
        pasajero_temp.id_pasajero, 
        pasajero_temp.pasajero, 
        pasajero_temp.edad, 
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