from sqlalchemy import create_engine
import pandas as pd
import traceback
from util.db_postgres import DB_Postgres as db

def load_associations():
    try:
        print("Iniciar Carga al sor")
        
        dbPostgres = db("staging")
        cursor = dbPostgres.start()  

        if cursor is None:
            raise Exception("No se pudo obtener un cursor válido")

        engine = create_engine(dbPostgres.connection_string())
        
        query = "SELECT * FROM tra_associations "
        associations_df = pd.read_sql(query, engine)
        

        queryUserStaging = "SELECT * FROM ext_users"
        users_sta_df = pd.read_sql(queryUserStaging, engine)
        
        dbPostgresSor = db("sor")
        cursorSor = dbPostgresSor.start()
        
        
        if cursorSor is None:
            raise Exception("No se pudo obtener un cursor válido para el Sor")
        
        engineSor = create_engine(dbPostgresSor.connection_string())  
        
        query_users = "SELECT * FROM dim_users"
        user_df = pd.read_sql(query_users, engineSor) 
        
        query_roles = "SELECT * FROM dim_roles"
        roles_df = pd.read_sql(query_roles, engineSor)
        
        query_companies = "SELECT * FROM dim_companies"
        companies_df = pd.read_sql(query_companies, engineSor)
        
 
        
        factAssociationsColumns = ['id_association', "id_user", 'id_company', 'id_role', "activated_at_id", "created_at_id", "email_verified_at_id", "verification_time_hours", "activate_time_hours", "is_business_mail"]

        existing_columns = [col for col in factAssociationsColumns if col in associations_df.columns]
        factAssociations_df = associations_df[existing_columns]

        
        for _, row in associations_df.iterrows():
            user = user_df.loc[user_df["id_users_bk"] == row['user_id']].iloc[0]
            
            company = companies_df.loc[companies_df["id_company_bk"] == row['company_id']].iloc[0]
            
            user_sta = users_sta_df[users_sta_df['id'] == user.id_users_bk].iloc[0]
            
            
            role = roles_df.loc[roles_df["id_roles_bk"] == user_sta['id_role']].iloc[0]
            
            factAssociations_df.loc[_, "id_user"] = user['id_users']
            factAssociations_df.loc[_, "id_company"] = company['id_company']
            factAssociations_df.loc[_, "id_role"] = role['id_roles']
            factAssociations_df.loc[_, "activated_at_id"] = user_sta['activated_at']
            factAssociations_df.loc[_, "created_at_id"] = user_sta['created_at']
            factAssociations_df.loc[_, "email_verified_at_id"] = user_sta['email_verified_at']
            factAssociations_df.loc[_, "verification_time_hours"] = row['verification_time_hours']
            factAssociations_df.loc[_, "activate_time_hours"] = row['activate_time_hours']
            factAssociations_df.loc[_, "is_business_mail"] = row['is_business_mail']

        factAssociations_df.to_sql('fact_associations', engineSor, if_exists='append', index=False)
        print("Finalizar carga al Sor")
        print("Finalizar carga al Sor")

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()
