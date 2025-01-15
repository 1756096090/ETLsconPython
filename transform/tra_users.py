import traceback
import pandas as pd
from sqlalchemy import create_engine
from util.db_postgres import DB_Postgres as db
from phonenumbers import geocoder, parse

def transform_users():
    try:
        print("Persistir Staging")
        dbPostgres = db("staging")
        dbPostgres.start()
        engine = create_engine(dbPostgres.connection_string())  

        query = f"SELECT * FROM ext_users"
        user_tra = pd.read_sql(query, engine)

        user_tra['email_verified_at'] = pd.to_datetime(user_tra['email_verified_at'])
        user_tra['created_at'] = pd.to_datetime(user_tra['created_at'])

        user_tra['verification_time_hours'] = (user_tra['email_verified_at'] - user_tra['created_at']).dt.total_seconds() / 3600
        user_tra['activate_time_hours'] = (user_tra['activated_at'] - user_tra['email_verified_at']).dt.total_seconds() / 3600
        
        def classify_email(email):
            personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
            domain = email.split('@')[-1]
            if domain.lower() in personal_domains:
                return "Correo personal"
            
            return "Correo empresarial"
        
        user_tra['is_business_mail'] = user_tra['email'].apply(classify_email)
        
        print(user_tra[['email_verified_at', 'created_at', 'verification_time_hours', 'is_business_mail', 'phone_code']])
    
    except Exception as e:
        print("Ocurrió un error:", str(e))
        traceback.print_exc()
