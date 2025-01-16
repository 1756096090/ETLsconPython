
from populate_db.roles import RoleManager
from populate_db.companies import CompanyManager
from populate_db.users import UserManager
from populate_db.associations import AssociationManager
from generateData.csv_json_data import read_csv, read_json

managerRole = RoleManager('oltp')
managerRole.save_roles(5)

managerCompany = CompanyManager('oltp')
managerCompany.save_companies(num_companies=50)

managerUsers = UserManager('oltp')
managerUsers.save_users(num_users=1000)

managerAss = AssociationManager('oltp')
managerAss.save_associations()






