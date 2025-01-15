import traceback
import pandas as pd
from sqlalchemy import create_engine
from util.db_postgres import DB_Postgres as db

def transform_companies():
    try:
        print("Generando la transformación de companies...")
        dbPostgres = db("staging")
        dbPostgres.start()
        engine = create_engine(dbPostgres.connection_string())  

        query_companies = "SELECT * FROM ext_companies"
        companies_tra = pd.read_sql(query_companies, engine)
        
        query_associations = "SELECT * FROM ext_associations"
        associations = pd.read_sql(query_associations, engine)

        companies_tra['total_associations'] = 0  # Inicializamos la columna

        for index, company in companies_tra.iterrows():  # iterrows() para iterar filas
            company_id = company['company_id']
            
            filter_associations = associations[associations['company_id'] == company_id]
            
            filter_size = len(filter_associations)  # Usar len() para contar filas
            companies_tra.at[index, 'total_associations'] = filter_size  # Actualizar fila específica
            
        print("Retornando comapanies_tra")
        return companies_tra

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()
