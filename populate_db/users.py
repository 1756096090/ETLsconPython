from faker import Faker
from util.db_postgres import DB_Postgres as db
from datetime import datetime
import random

class User:
    def __init__(
        self, id, salesforce_id, name, email, last_verification_email_sent,
        email_verified_at, password, country, phone_code, phone_number,
        remember_token, created_at, updated_at, activated_at, website,
        zip_code, first_name, last_name, job_role, user_ip, terms_accepted,
        status, id_role
    ):
        self.id = id
        self.salesforce_id = salesforce_id
        self.name = name
        self.email = email
        self.last_verification_email_sent = last_verification_email_sent
        self.email_verified_at = email_verified_at
        self.password = password
        self.country = country
        self.phone_code = phone_code
        self.phone_number = phone_number
        self.remember_token = remember_token
        self.created_at = created_at
        self.updated_at = updated_at
        self.activated_at = activated_at
        self.website = website
        self.zip_code = zip_code
        self.first_name = first_name
        self.last_name = last_name
        self.job_role = job_role
        self.user_ip = user_ip
        self.terms_accepted = terms_accepted
        self.status = status
        self.id_role = id_role

class UserManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)
        self.fake = Faker()

    def generate_id(self) -> int:
        timestamp = int(datetime.now().strftime("%m%d%H%M%S%f")[:-3])  # Milisegundos
        random_part = random.randint(100, 999)  # Número aleatorio de 3 dígitos
        return int(f"{timestamp}{random_part}")

    def generate_users(self, num_users: int = 10) -> list:
        users = []
        for _ in range(num_users):
            user_id = self.generate_id()
            salesforce_id = self.fake.uuid4()[:8]
            name = self.fake.name()
            email = self.fake.email()
            last_verification_email_sent = self.fake.date_time_this_year()
            email_verified_at = self.fake.date_time_this_year()
            password = self.fake.password()
            country = self.fake.country()
            phone_code = f"+{random.randint(1, 999)}"
            phone_number = self.fake.phone_number()
            remember_token = self.fake.uuid4()[:20]
            created_at = self.fake.date_time_this_year()
            updated_at = self.fake.date_time_this_year()
            activated_at = self.fake.date_time_this_year()
            website = self.fake.url()
            zip_code = self.fake.zipcode()
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            job_role = self.fake.job()
            user_ip = self.fake.ipv4()
            terms_accepted = self.fake.boolean(chance_of_getting_true=90)
            status = self.fake.boolean(chance_of_getting_true=80)
            id_role = random.randint(1, 5)  # Ejemplo: ID de rol entre 1 y 5

            user = User(
                user_id, salesforce_id, name, email, last_verification_email_sent,
                email_verified_at, password, country, phone_code, phone_number,
                remember_token, created_at, updated_at, activated_at, website,
                zip_code, first_name, last_name, job_role, user_ip, terms_accepted,
                status, id_role
            )

            users.append((
                user.id, user.salesforce_id, user.name, user.email,
                user.last_verification_email_sent, user.email_verified_at,
                user.password, user.country, user.phone_code, user.phone_number,
                user.remember_token, user.created_at, user.updated_at,
                user.activated_at, user.website, user.zip_code, user.first_name,
                user.last_name, user.job_role, user.user_ip,
                user.terms_accepted, user.status, user.id_role
            ))
        return users

    def save_users(self, num_users: int = 10):
        users = self.generate_users(num_users)
        try:
            self.db_connection.start()
            cursor = self.db_connection.cursor
            query = """
            INSERT INTO users (
                id, salesforce_id, name, email, last_verification_email_sent_at,
                email_verified_at, password, country, phone_code, phone_number,
                remember_token, created_at, updated_at, activated_at, website,
                zip_code, first_name, last_name, job_role, user_ip, terms_accepted,
                status, id_role
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, users)
            self.db_connection.connection.commit()
            print(f"{len(users)} usuarios insertados con éxito en la base de datos.")
        except Exception as e:
            print(f"Error al guardar los usuarios: {e}")
        finally:
            self.db_connection.stop()