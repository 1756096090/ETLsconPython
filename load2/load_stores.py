import traceback
import pandas as pd
from util.db_connection import Db_Connection
def cargar_stores():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'root'
        db = 'staging'
        
        con_sta_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt =  "select  store_id, name, city, country  \
                    from ext_fim"
        stores_tra = pd.read_sql (sql_stmt, ses_sta_db)
        
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = 'root'
        db = 'sor'
        
        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        dim_sto_dict =  {
            "store_bk": [],
            "name": [],
            "city": [],
            "country": []
        }
        
        if not stores_tra.empty:
            for bk, nam, cit, cou  \
            in zip(stores_tra['store_id'], stores_tra['name'], stores_tra['city'],stores_tra['country']):
                dim_sto_dict['store_bk'].append(bk)
                dim_sto_dict['name'].append(nam)
                dim_sto_dict['city'].append(cit)
                dim_sto_dict['country'].append(cou) 
        print(dim_sto_dict)        
        
        if dim_sto_dict['store_bk']:
            dim_sto = pd.DataFrame(dim_sto_dict)
            dim_sto.to_sql('dim_store', ses_sor_db, index=False, if_exists='append')
                
        
    except:
        traceback.print_exc()
    finally:
        pass
    
    