from google.cloud import bigquery

def carga_pasajero_final ( dataset_origen, tabla_origen, dataset_destino, tabla_destino ):

    # Initialize a BigQuery client
    client = bigquery.Client()

    # Define the SQL for the stored procedure
    sql = f"""
    MERGE `{dataset_destino}.{tabla_destino}` AS vuelos
    USING `{dataset_origen}.{tabla_origen}`  AS vuelos_temp
    ON  vuelo.sucursal = vuelo_temp.sucursal
    AND vuelo.cve_la = vuelo_temp.cve_la
    AND vuelo.ruta = vuelo_temp.ruta
    AND vuelo.cve_cliente = vuelo_temp.cve_cliente
    WHEN MATCHED THEN
    UPDATE SET 
        vuelos.viaje = vuelos_temp.pasajero, 
        vuelos.clase = vuelos_temp.edad, 
        vuelos.precio = vuelos_temp.precio,
        vuelos.fecha_actualizacion = CURRENT_DATE
    WHEN NOT MATCHED THEN
    INSERT (
        sucursal,
        cve_la,
        ruta,
        cve_cliente,
        viaje,
        clase,
        precio,
        fecha_creacion, 
        fecha_actualizacion
    ) VALUES (
        vuelos_temp.sucursal,
        vuelos_temp.cve_la,
        vuelos_temp.ruta,
        vuelos_temp.cve_cliente,
        vuelos_temp.viaje,
        vuelos_temp.clase,
        vuelos_temp.precio,
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