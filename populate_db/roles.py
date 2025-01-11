from faker import Faker
from util.db_postgres import DB_Postgres as db
import random
import traceback

class Role:
    def __init__(self, name: str, guard_name: str):
        self.name = name
        self.guard_name = guard_name


class RoleManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)
        self.fake = Faker()

    def generate_roles(self, num_roles: int = 10) -> list:
        """Genera una lista de roles aleatorios."""
        guard_names = ['api', 'web']
        roles = []
        for _ in range(num_roles):
            name = self.fake.job()  
            guard_name = random.choice(guard_names)
            roles.append((name, guard_name))  # Corrige el formato para que sea una tupla
        return roles

    def save_roles(self, num_roles: int = 10):
        """Genera y guarda roles en la base de datos."""
        roles = self.generate_roles(num_roles)
        try:
            self.db_connection.start()  
            cursor = self.db_connection.cursor
            query = """
            INSERT INTO roles (name, guard_name)
            VALUES (%s, %s)
            """
            cursor.executemany(query, roles)  
            self.db_connection.connection.commit()  # Confirma la transacción
            print(f"{len(roles)} roles insertados con éxito en la base de datos.")
        except Exception as e:
            print("Error al guardar los roles:")
            traceback.print_exc()  
        finally:
            self.db_connection.stop()  #