from faker import Faker
from util.db_postgres import DB_Postgres as db
from datetime import datetime, timedelta
import random

class User:
    def __init__(self, **kwargs):
        self.salesforce_id = kwargs.get('salesforce_id')
        self.name = kwargs.get('name')
        self.email = kwargs.get('email')
        self.last_verification_email_sent = kwargs.get('last_verification_email_sent')
        self.email_verified_at = kwargs.get('email_verified_at')
        self.password = kwargs.get('password')
        self.country = kwargs.get('country')
        self.phone_code = kwargs.get('phone_code')
        self.phone_number = kwargs.get('phone_number')
        self.remember_token = kwargs.get('remember_token')
        self.created_at = kwargs.get('created_at')
        self.updated_at = kwargs.get('updated_at')
        self.activated_at = kwargs.get('activated_at')
        self.website = kwargs.get('website')
        self.zip_code = kwargs.get('zip_code')
        self.first_name = kwargs.get('first_name')
        self.last_name = kwargs.get('last_name')
        self.job_role = kwargs.get('job_role')
        self.user_ip = kwargs.get('user_ip')
        self.terms_accepted = kwargs.get('terms_accepted')
        self.status = kwargs.get('status')
        self.id_role = kwargs.get('id_role')

class UserManager:
    def __init__(self, database: str):
        self.database = database
        self.db_connection = db(database)
        self.fake = Faker()

    def generate_id(self) -> int:
        timestamp = int(datetime.now().strftime("%m%d%H%M%S%f")[:-3])
        random_part = random.randint(100, 999)
        return int(f"{timestamp}{random_part}")

    def generate_users(self, num_users: int = 10) -> list:
        users = []
        for _ in range(num_users):
            created_at = self.fake.date_time_between(start_date='-1y', end_date='now')
            email_verified_at = created_at + timedelta(minutes=random.randint(10, 1440))  # Entre 10 minutos y 24 horas
            activated_at = email_verified_at + timedelta(minutes=random.randint(5, 720))  # Entre 5 minutos y 12 horas
            last_verification_email_sent = self.fake.date_time_between(start_date=created_at, end_date=email_verified_at)

            email_domain = random.choice(['@gmail.com', '@yahoo.com', '@hotmail.com', '@outlook.com', f"@{self.fake.domain_name()}"])
            email = f"{self.fake.user_name()}{random.randint(10, 1440)}{email_domain}"

            user = User(
                salesforce_id=self.fake.uuid4(),
                name=self.fake.name(),
                email=email,
                last_verification_email_sent=last_verification_email_sent,
                email_verified_at=email_verified_at,
                password=self.fake.password(length=12),
                country=self.fake.country(),
                phone_code=f"+{random.randint(1, 999)}",
                phone_number=self.fake.msisdn(),
                remember_token=self.fake.md5(raw_output=False),
                created_at=created_at,
                updated_at=created_at + timedelta(days=random.randint(1, 10)),
                activated_at=activated_at,
                website=self.fake.url(),
                zip_code=self.fake.zipcode(),
                first_name=self.fake.first_name(),
                last_name=self.fake.last_name(),
                job_role=self.fake.job(),
                user_ip=self.fake.ipv4(),
                terms_accepted=True,
                status=True,
                id_role=random.randint(1, 5)
            )

            users.append((user.salesforce_id, user.name, user.email,
                user.last_verification_email_sent, user.email_verified_at, user.password,
                user.country, user.phone_code, user.phone_number, user.remember_token,
                user.created_at, user.updated_at, user.activated_at, user.website,
                user.zip_code, user.first_name, user.last_name, user.job_role,
                user.user_ip, user.terms_accepted, user.status, user.id_role))

        return users

    def save_users(self, num_users: int = 10):
        users = self.generate_users(num_users)
        try:
            # Abrir la conexión con la base de datos
            self.db_connection.start()
            cursor = self.db_connection.connection.cursor()

            # Consulta SQL para insertar usuarios
            query = """
            INSERT INTO users (salesforce_id, name, email, last_verification_email_sent_at, 
                               email_verified_at, password, country, phone_code, phone_number, 
                               remember_token, created_at, updated_at, activated_at, website, 
                               zip_code, first_name, last_name, job_role, user_ip, terms_accepted, 
                               status, id_role)
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.executemany(query, users)
            self.db_connection.connection.commit()  # Confirmar transacción

            print(f"{len(users)} usuarios insertados con éxito en la base de datos.")
        except Exception as e:
            print(f"Error al guardar los usuarios: {e}")
        finally:
            # Asegurarse de cerrar la conexión
            self.db_connection.stop()
