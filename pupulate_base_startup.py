
from populate_db.roles import RoleManager
from populate_db.companies import CompanyManager
from populate_db.users import UserManager
#role_manager = RoleManager('oltp')
#role_manager.save_roles(5)

# manager = CompanyManager('oltp')
# manager.save_companies(num_companies=50)

manager = UserManager('oltp')
manager.save_users(num_users=1000)


# import populate_db.roles as roles


