import traceback
import pandas as pd
from util.db_connection import Db_Connection
def transformar_film():
    try:
        con_db = Db_Connection('oltp')
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt =  "SELECT * FROM ext_film"
        film_tra = pd.read_sql(sql_stmt, ses_db)
        
        film_tra['length'] = film_tra['length'].astype(str)
        
        def categorize_length(length):
            length = int(length)
            if length < 60:
                return '< 1h'
            elif 60 <= length < 90:
                return '< 1.5h'
            elif 90 <= length < 120:
                return '< 2h'
            else:
                return '> 2h'
        
        film_tra['length'] = film_tra['length'].apply(categorize_length)
        
        return film_tra
                
    except:
        traceback.print_exc()
    finally:
        pass
    
    