from util.db_postgres import DB_Postgres as db
import random

class AssociationManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)

    def fetch_existing_ids(self):
        """
        Recupera los IDs existentes de las tablas `users` y `companies` para asegurar la integridad referencial.
        """
        try:
            self.db_connection.start()
            cursor = self.db_connection.cursor

            #Recuperar todos los IDs de usuarios
            cursor.execute("SELECT id FROM users")
            user_ids = [row[0] for row in cursor.fetchall()]

            #Recuperar todos los IDs de compañías
            cursor.execute("SELECT company_id FROM companies")
            company_ids = [row[0] for row in cursor.fetchall()]

            return user_ids, company_ids

        except Exception as e:
            print(f"Error al recuperar IDs: {e}")
            return [], []

        finally:
            self.db_connection.stop()

    def generate_associations(self, num_associations: int, user_ids: list, company_ids: list):
        """
        Genera asociaciones aleatorias entre usuarios y empresas.
        """
        associations = set()  # Usamos un conjunto para evitar duplicados
        while len(associations) < num_associations:
            user_id = random.choice(user_ids)
            company_id = random.choice(company_ids)
            associations.add((user_id, company_id))
        return list(associations)

    def save_associations(self, num_associations: int = 50):
        """
        Guarda las asociaciones en la tabla `associations`.
        """
        user_ids, company_ids = self.fetch_existing_ids()
        if not user_ids or not company_ids:
            print("No se encontraron usuarios o compañías en la base de datos. Asegúrate de poblar estas tablas primero.")
            return

        associations = self.generate_associations(num_associations, user_ids, company_ids)

        try:
            self.db_connection.start()
            cursor = self.db_connection.cursor
            query = """
            INSERT INTO associations (user_id, company_id)
            VALUES (%s, %s)
            """
            cursor.executemany(query, associations)
            self.db_connection.connection.commit()
            print(f"{len(associations)} asociaciones insertadas con éxito en la base de datos.")

        except Exception as e:
            print(f"Error al guardar las asociaciones: {e}")

        finally:
            self.db_connection.stop()


# if __name__ == "__main__":
#     manager = AssociationManager("oltp")
#     manager.save_associations(num_associations=50)
