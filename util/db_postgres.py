import psycopg2

host = "191.235.234.131"
port = "5432"
dbname = "oltp"
user = "postgres"
password = "123456789"

class DB_Postgres:
    
    def __init__(self, database):
        self.database = database
        self.connection = None
        self.cursor = None
        
    def start(self):
        try:
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                dbname=self.database,
                user=user,
                password=password
            )
            self.cursor = self.connection.cursor()
            print("Conexión exitosa a la base de datos")
        
        except Exception as error:
            print(f"Error al conectar a la base de datos: {error}")

    def stop(self):
        try:
            # Cerrar la conexión y el cursor
            if self.connection:
                self.cursor.close()
                self.connection.close()
                print("Conexión cerrada")
        except Exception as error:
            print(f"Error al cerrar la conexión: {error}")

