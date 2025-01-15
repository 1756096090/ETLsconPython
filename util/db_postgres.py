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
        """Inicia la conexión si no está activa"""
        if self.connection is None:
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
                return self.cursor
            except Exception as error:
                print(f"Error al conectar a la base de datos: {error}")
                self.connection = None
                self.cursor = None
        else:
            print("La conexión ya está activa.")
    
    def stop(self):
        """Cierra la conexión y el cursor de manera segura"""
        if self.connection:
            try:
                self.cursor.close()
                self.connection.close()
                print("Conexión cerrada")
            except Exception as error:
                print(f"Error al cerrar la conexión: {error}")
            finally:
                self.connection = None
                self.cursor = None

    def execute_query(self, query, params=None):
        """Ejecuta una consulta SQL y devuelve los resultados"""
        if self.connection is None:
            print("No hay conexión activa")
            return None
        try:
            self.cursor.execute(query, params)
            # Commit para consultas que modifican la base de datos
            self.connection.commit()
            return self.cursor.fetchall()
        except Exception as error:
            print(f"Error al ejecutar la consulta: {error}")
            return None

    def connection_string(self):
        """Devuelve la cadena de conexión para usar con SQLAlchemy o en otros lugares"""
        return f"postgresql://{user}:{password}@{host}:{port}/{self.database}"

