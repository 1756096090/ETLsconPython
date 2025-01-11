from faker import Faker
from util.db_postgres import DB_Postgres as db
from datetime import datetime
import random

class Company:
    def __init__(self, name: str, address: str, phone_number: str, ruc: str):
        self.name = name
        self.address = address
        self.phone_number = phone_number
        self.ruc = ruc

class CompanyManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)
        self.fake = Faker('es_EC')  

    def generate_ruc(self) -> str:
        #generar RUC con 10 dígitos aleatorios + '001'
        cedula = ''.join([str(random.randint(0, 9)) for _ in range(10)])
        return f"{cedula}001"

    def generate_companies(self, num_companies: int = 10) -> list:
        companies = []
        ruc_set = set()#evitar duplicados de RUC
        for _ in range(num_companies):
            while True:
                ruc = self.generate_ruc()
                if ruc not in ruc_set:#evitar duplicados
                    ruc_set.add(ruc)
                    break
            name = self.fake.company()
            address = self.fake.address()
            phone_number = self.fake.phone_number()
            company = Company(name, address, phone_number, ruc)
            companies.append((company.name, company.address, company.phone_number, company.ruc))
        return companies

    def save_companies(self, num_companies: int = 10):
        companies = self.generate_companies(num_companies)
        try:
            self.db_connection.start()
            cursor = self.db_connection.cursor
            query = """
            INSERT INTO companies (name, address, phone_number, ruc)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """
            cursor.executemany(query, companies)
            self.db_connection.connection.commit()
            print(f"{len(companies)} empresas insertadas con éxito en la base de datos.")
        except Exception as e:
            print(f"Error al guardar las empresas: {e}")
        finally:
            self.db_connection.stop()


if __name__ == "__main__":
    manager = CompanyManager("nombre_de_tu_base_de_datos")
    manager.save_companies(num_companies=50)
