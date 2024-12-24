from faker import Faker
from util.db_postgres import DB_Postgres as db
from datetime import datetime
import random

class Role:
    def __init__(self, role_id: str, name: str, guard_name: str):
        self.role_id = role_id
        self.name = name
        self.guard_name = guard_name


class RoleManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)
        self.fake = Faker()

    def generate_id(self) -> int:
        timestamp = int(datetime.now().strftime("%m%d%H%M%S%f")[:-3])  # Milisegundos
        random_part = random.randint(100, 999)  # Número aleatorio de 3 dígitos
        return int(f"{timestamp}{random_part}")

    def generate_roles(self, num_roles: int = 10) -> list:
        guard_names = ['api', 'web']
        roles = []
        for _ in range(num_roles):
            role_id = self.generate_id()
            name = self.fake.job()
            guard_name = random.choice(guard_names)
            role = Role(role_id, name, guard_name)
            roles.append((role.role_id, role.name, role.guard_name))
        return roles

    def save_roles(self, num_roles: int = 10):
        roles = self.generate_roles(num_roles)
        try:
            self.db_connection.start()
            cursor = self.db_connection.cursor
            query = """
            INSERT INTO roles (id, name, guard_name)
            VALUES (%s, %s, %s)
            """
            cursor.executemany(query, roles)
            self.db_connection.connection.commit()
            print(f"{len(roles)} roles insertados con éxito en la base de datos.")
        except Exception as e:
            print(f"Error al guardar los roles: {e}")
        finally:
            self.db_connection.stop()


