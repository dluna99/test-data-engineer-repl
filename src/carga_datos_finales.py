import carga_linea_aerea_final
import carga_vuelos_final
import carga_pasajero_final

def cargar_datos_fiknales( dataset, tabla, folder ):
    
    #Dataset origen 
    mi_dataset_origen = 'silver'
    #Dataset destino 
    mi_dataset_destino = 'gold'

    #Carga líneas aereas 
    carga_linea_aerea_final( mi_dataset_origen, 'linea_aerea_tmp', mi_dataset_destino, 'linea_aerea'  )

    #Carga Sucursales
    #Carga Pasajeros
    carga_pasajero_final( mi_dataset_origen, 'pasajero_dedup', mi_dataset_destino, 'pasajero'  )

    #Carga vuelos 
    carga_vuelos_final( mi_dataset_origen, 'vuelos_tmp', mi_dataset_destino, 'vuelo'  )

