import traceback
import pandas as pd
from util.db_connection2 import Db_Connection
def cargar_film():
    try:
        con_sta_db = Db_Connection('staging')
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt =  "select  film_id, title, release_year, length, rating \
                    from ext_film"
        ext_film = pd.read_sql (sql_stmt, ses_sta_db)
        
     
        
        con_sor_db = Db_Connection('sor')
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        dim_film_dict =  {
            "film_bk": [],
            "title": [],
            "release_year": [],
            "length": [],
            "rating": [],
        }
        
        if not ext_film.empty:
            for bk, tit, rel, len, rat  \
            in zip(ext_film['film_id'], ext_film['title'], ext_film['release_year'],ext_film['length'], ext_film['rating']):
                dim_film_dict['film_bk'].append(bk),
                dim_film_dict['title'].append(tit),
                dim_film_dict['release_year'].append(rel),
                dim_film_dict['length'].append(len),
                dim_film_dict['rating'].append(rat)
        print(dim_film_dict)        
        
        if dim_film_dict['film_bk']:
            dim_sto = pd.DataFrame(dim_film_dict)
            dim_sto.to_sql('dim_film', ses_sor_db, index=False, if_exists='append')
                
        
    except:
        traceback.print_exc()
    finally:
        pass
    
    