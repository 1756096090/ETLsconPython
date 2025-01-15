import traceback
import pandas as pd
from util.db_connection2 import Db_Connection
def cargar_fact_inventory():
    try:
        con_sta_db = Db_Connection('staging')
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")
        
        sql_stmt = "SELECT inventory_id, film_id, store_id, last_update FROM ext_inventory"
        
        fact_inventory = pd.read_sql(sql_stmt, ses_sta_db)
        
        
        con_sor_db = Db_Connection('sor')
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos ")

        dim_inventory_dict = {
            "store_id": [],
            "film_id": [],
            "date_id": [],
            "rental_price": [],
            "rental_cost": [],
        
        }

        if not fact_inventory.empty:
            for fi, st, di in zip(fact_inventory['film_id'], fact_inventory['store_id'],
                                  fact_inventory['last_update']):
                
                sql_film = f"SELECT id FROM dim_film WHERE film_bk = {fi}"
                dim_film = pd.read_sql(sql_film, ses_sor_db)                
                if not dim_film.empty:
                    dim_inventory_dict['film_id'].append(dim_film.iloc[-1]['id'])

                sql_store = f"SELECT id FROM dim_store WHERE store_bk = {st}"
                dim_store = pd.read_sql(sql_store, ses_sor_db)
                if not dim_store.empty:
                    dim_inventory_dict['store_id'].append(dim_store.iloc[-1]['id'])

                sql_date = f"SELECT id FROM dim_date WHERE date_bk = '{di.date()}'"
                dim_date = pd.read_sql(sql_date, ses_sor_db)
                if not dim_date.empty:
                    dim_inventory_dict['date_id'].append(dim_date.iloc[-1]['id'])

                sql_filmRental = f"SELECT replacement_cost, rental_rate FROM tra_film WHERE film_id = {fi}"
                tra_film = pd.read_sql(sql_filmRental, ses_sta_db)
                if not tra_film.empty:
                    dim_inventory_dict['rental_price'].append(tra_film.iloc[-1]['rental_rate'])
                    dim_inventory_dict['rental_cost'].append(tra_film.iloc[-1]['replacement_cost'])

        print(dim_inventory_dict)
        if dim_inventory_dict['film_id']: 
            dim_inv = pd.DataFrame(dim_inventory_dict)
            dim_inv.to_sql('fact_inventory', ses_sor_db, index=False, if_exists='append')      
        
    except:
        traceback.print_exc()
    finally:
        pass
    
    