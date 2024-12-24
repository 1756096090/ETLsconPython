from populate_db.roles import RoleManager

role_manager = RoleManager('oltp')
role_manager.save_roles(5)
