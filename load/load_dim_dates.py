from sqlalchemy import create_engine
import pandas as pd
from util.db_postgres import DB_Postgres as db

def load_dates():
    try:
        print("Iniciando carga optimizada de usuarios y fechas al sistema SOR")
        
        # Establecer conexiones a las bases de datos
        staging_engine = create_engine(db("staging").connection_string())
        sor_engine = create_engine(db("sor").connection_string())
        
        # Obtener datos de usuarios
        query = """
        SELECT id, activated_at, created_at, email_verified_at
        FROM ext_users
        """
        users_df = pd.read_sql(query, staging_engine)
        
        date_columns = ['activated_at', 'created_at', 'email_verified_at']
        for col in date_columns:
            users_df[col] = pd.to_datetime(users_df[col], errors='coerce')
        
        date_user_rows = []
        
        for col in date_columns:
            valid_dates = users_df[['id', col]].dropna()
            
            for _, row in valid_dates.iterrows():
                date_user_rows.append({
                    'date': row[col],
                    'id_user': row['id'],
                    'date_type': col
                })
        
        # Convertir la lista de filas a un DataFrame
        all_dates_df = pd.DataFrame(date_user_rows)
        
        if not all_dates_df.empty:
            unique_dates = all_dates_df['date'].unique()
            date_ids = {date: int(date.timestamp()) for date in unique_dates}  
            
            dim_dates = pd.DataFrame({
                'id_date': unique_dates,  
                'hour': [date.hour for date in unique_dates],
                'minute': [date.minute for date in unique_dates],
                'second': [date.second for date in unique_dates],
                'day': [date.day for date in unique_dates],
                'month': [date.month for date in unique_dates],
                'year': [date.year for date in unique_dates],
                'quarter': [date.quarter for date in unique_dates],
                'day_of_week': [date.weekday() for date in unique_dates],
                'is_weekend': [(date.weekday() >= 5) for date in unique_dates],
                'day_of_year': [date.dayofyear for date in unique_dates]
            }).sort_values(by='id_date')
            
            print(dim_dates)
            
            fact_user_dates = pd.DataFrame({
                'id_user': all_dates_df['id_user'],
                'id_date': all_dates_df['date'].map(date_ids),
                'date_type': all_dates_df['date_type'],
            })
            
            print(fact_user_dates)
            
            # Guardar las fechas en la base de datos SOR
            dim_dates.to_sql("dim_dates", sor_engine, if_exists='append', index=False)
        
        print("Finalizada la carga de usuarios y fechas al SOR")
        
    except Exception as e:
        print(f"Error en la carga: {str(e)}")
        raise

