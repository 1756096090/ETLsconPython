import traceback
import pandas as pd
from sqlalchemy import create_engine
from util.db_postgres import DB_Postgres as db

def transform_associations():
    try:
        print("Generando la transformación de associations...")
        dbPostgres = db("staging")
        dbPostgres.start()
        engine = create_engine(dbPostgres.connection_string())

        query_users = "SELECT * FROM ext_users"
        users = pd.read_sql(query_users, engine)

        query_associations = "SELECT * FROM ext_associations"
        associations_tra = pd.read_sql(query_associations, engine)
        
        def classify_email(email):
            resp = True
            personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
            domain = email.split('@')[-1]
            if domain.lower() in personal_domains:
                resp = False
            return resp
        

        users['email_verified_at'] = pd.to_datetime(users['email_verified_at'])
        users['created_at'] = pd.to_datetime(users['created_at'])
        users['activated_at'] = pd.to_datetime(users['activated_at'])

        users['verification_time_hours'] = (users['email_verified_at'] - users['created_at']).dt.total_seconds() / 3600
        users['activate_time_hours'] = (users['activated_at'] - users['email_verified_at']).dt.total_seconds() / 3600
        users['is_business_mail'] =  users['email'].apply(classify_email)
        
        
        

        associations_tra['verification_time_hours'] = associations_tra['user_id'].map(
            users.set_index('id')['verification_time_hours']
        )
        associations_tra['activate_time_hours'] = associations_tra['user_id'].map(
            users.set_index('id')['activate_time_hours']
        )
        
        associations_tra['is_business_mail'] = associations_tra['user_id'].map(
            users.set_index('id')['is_business_mail']
        )

        print("Transformación completada.")
        print(associations_tra[['verification_time_hours', 'activate_time_hours', 'is_business_mail']])

        return associations_tra

    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()


