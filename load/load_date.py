import traceback
import pandas as pd
from util.db_connection2 import Db_Connection
def cargar_date():
    try:
        con_sta_db = Db_Connection('staging')
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt = "SELECT inventory_id, last_update FROM ext_inventory"
        ext_inventory = pd.read_sql(sql_stmt, ses_sta_db)
        
        con_sor_db = Db_Connection('sor')
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")

        dim_inventory_dict = {
            "date_bk": [],
            "date_month": [],
            "date_year": [],
        }

        if not ext_inventory.empty:
            ext_inventory['last_update'] = pd.to_datetime(ext_inventory['last_update'])
            dim_inventory_dict['date_bk'] = ext_inventory['last_update'].dt.date.tolist() 
            dim_inventory_dict['date_month'] = ext_inventory['last_update'].dt.strftime('%B').tolist() 
            dim_inventory_dict['date_year'] = ext_inventory['last_update'].dt.year.tolist() 

        
        if dim_inventory_dict['date_bk']:
            dim_sto = pd.DataFrame(dim_inventory_dict).drop_duplicates() 
            dim_sto = pd.DataFrame(dim_inventory_dict)
            dim_sto.to_sql('dim_date', ses_sor_db, index=False, if_exists='append')
                
        
    except:
        traceback.print_exc()
    finally:
        pass
    
    