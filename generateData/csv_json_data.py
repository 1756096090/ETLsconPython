import csv
import json
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()

def generate_ruc() -> str:
    # Generar un RUC con 10 dígitos aleatorios + '001'
    cedula = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    return f"{cedula}001"

def generate_csv_data(file_path: str, num_records: int = 40):
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'salesforce_id', 'name', 'email', 'last_verification_email_sent_at', 
            'email_verified_at', 'password', 'country', 'phone_code', 'phone_number', 
            'remember_token', 'created_at', 'updated_at', 'activated_at', 'website', 
            'zip_code', 'first_name', 'last_name', 'job_role', 'user_ip', 
            'terms_accepted', 'status', 'id_role'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_records):
            created_at = fake.date_time_between(start_date='-1y', end_date='now')
            email_verified_at = created_at + timedelta(minutes=random.randint(10, 1440))  # 10 min to 24 hrs later
            activated_at = email_verified_at + timedelta(minutes=random.randint(5, 720))  # 5 min to 12 hrs later
            
            email_domain = random.choice(['@gmail.com', '@yahoo.com', '@hotmail.com', '@outlook.com', f"@{fake.domain_name()}"])
            email = f"{fake.user_name()}{random.randint(10, 1440)}{email_domain}"
            
            writer.writerow({
                'salesforce_id': fake.uuid4(),
                'name': fake.name(),
                'email': email,
                'last_verification_email_sent_at': email_verified_at.isoformat(),
                'email_verified_at': email_verified_at.isoformat(),
                'password': fake.password(length=10),
                'country': fake.country(),
                'phone_code': f"+{random.randint(1, 999)}",
                'phone_number': fake.phone_number(),
                'remember_token': fake.uuid4(),
                'created_at': created_at.isoformat(),
                'updated_at': datetime.now().isoformat(),
                'activated_at': activated_at.isoformat(),
                'website': fake.url(),
                'zip_code': fake.zipcode(),
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'job_role': fake.job(),
                'user_ip': fake.ipv4_public(),
                'terms_accepted': fake.boolean(chance_of_getting_true=80),
                'status': fake.boolean(chance_of_getting_true=90),
                'id_role': random.randint(1, 5),
            })

def generate_json_data(file_path: str, num_records: int = 40):
    data = []
    generated_rucs = set()  # Para evitar RUCs duplicados

    for _ in range(num_records):
        # Generar un RUC único
        while True:
            ruc = generate_ruc()
            if ruc not in generated_rucs:
                generated_rucs.add(ruc)
                break

        data.append({
            'name': fake.company(),
            'address': fake.address(),
            'phone_number': fake.phone_number(),
            'ruc': ruc
        })
    
    with open(file_path, mode='w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, indent=4)

def read_csv(file_path: str):
    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row)

def read_json(file_path: str):
    with open(file_path, mode='r', encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
        for record in data:
            print(record)

# File paths
csv_file_path = 'users_data.csv'
json_file_path = 'company_data.json'

# Generate data
generate_csv_data(csv_file_path)
generate_json_data(json_file_path)

# Read data
print("CSV Data:")
read_csv(csv_file_path)

print("\nJSON Data:")
read_json(json_file_path)