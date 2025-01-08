import carga_datos
import carga_lineas_aereas
import carga_vuelos
import carga_pasajeros

def cargar_archivos( dataset, tabla, folder ):
    
    #Carga líneas aereas 
    mi_folder = '../data/central/CatLineasAereas.csv'
    carga_lineas_aereas( 'silver', 'linea_aerea_tmp', mi_folder  )

    #Carga Sucursales
    #Carga Pasajeros
    mi_folder = '../data/sucursal1/pasajeros.csv'
    carga_pasajeros( 'silver', 'pasajero_tmp', mi_folder, truncar_table=True )

    mi_folder = './data/sucursal2/pasajeros.csv'
    carga_pasajeros( 'silver', 'pasajero_tmp', mi_folder, truncar_table=False )

    #Se deduplican los pasajeros 
    pasajeros_deduplicados = carga_pasajeros.deduplicar_pasajeros( 'silver', 'pasajero_tmp', 'pasajero_dedup' )

    #Se crea la tabla con los pasajeros únicos
    carga_pasajeros.crear_tabla( 'silver', 'pasajero_dedup', pasajeros_deduplicados)

    #Carga vuelos 
    mi_folder = '../data/sucursal1/vuelos.csv'
    carga_vuelos( 'silver', 'vuelo_tmp', mi_folder, sucursal=1 )

    #Tener en cuenta que en el segundo archivo no está presente la columna sucursal
    mi_folder = '../data/sucursal2/vuelos.csv'
    carga_vuelos( 'silver', 'vuelo_tmp', mi_folder, sucursal=2 )
    
