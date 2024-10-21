from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.ext_address import extraer_address
from extract.ext_city import extraer_city
from extract.ext_film import extraer_film
from extract.ext_inventory import extraer_inventory
from extract.ext_date import extraer_date
from extract.per_staging import persistir_staging
from util.db_connection import Db_Connection
from transform.tra_stores import transformar_stores
from transform.tra_film import transformar_film
from load.load_stores import cargar_stores
from load.load_film import cargar_film
import traceback


try:
    # print('Extrayendo dato desde un csv')
    # print('Extrayendo countries')
    # countries = extraer_countries()
    # # print(countries)
    # print('Persistiendo countries en staging')
    # persistir_staging(countries, 'ext_country')
    
    # print('Extrayendo date');
    # date = extraer_date()
    # # print(date)
    # print('Persistiendo date en staging')
    # persistir_staging(date, 'ext_date')
    
    # print('Extrayendo datos de la base de datos')
    # print('Extrayendo address')
    # address = extraer_address()
    # # print(address)
    # print('Persistiendo address en staging')
    # persistir_staging(address, 'ext_address')
    
    # print('Extrayendo city')
    # city = extraer_city()
    # # print(city)
    # print('Persistiendo city en staging')
    # persistir_staging(city, 'ext_city')
    
    # print('Extrayendo film')
    # film = extraer_film()
    # # print(film)
    # print('Persistiendo film en staging')
    # persistir_staging(film, 'ext_film')
    
    # print('Extrayendo inventory')
    # inventory = extraer_inventory()
    # # print(inventory)
    # print('Persistiendo inventory en staging')
    # persistir_staging(inventory, 'ext_inventory')
    
    # print('Extrayendo stores')
    # stores = extraer_stores()   
    # # print(stores)
    # persistir_staging(stores, 'ext_store')
    
    # print('Trasformado datos de STORES en els staging') 
    # tra_stores = transformar_stores()
    # persistir_staging(tra_stores, 'tra_store')
    # print('Trasformado datos de FILM en els staging')
    # tra_film = transformar_film()
    # print(tra_film)
    # print("persistir tra_film en staging")
    # persistir_staging(tra_film, 'tra_film')
    
    # # print(tra_stores)
    # print('Cargando datos de staging a stores')
    # cargar_stores()
    # print('Cargando datos de FILM en els staging')
    # cargar_film()
    
    
    
    

except:
    traceback.print_exc()
finally:
    pass
    # ses_db_oltp = con_db.stop()