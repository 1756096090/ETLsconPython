from extract.extract import extract
from extract.per_staging import persistir_staging
from transform.tra_companies import transform_companies
from transform.tra_associations import transform_associations
from load.load_dim_roles import load_roles
from load.load_dim_companies import load_companies
from load.load_dim_users import load_users
from load.load_dim_dates import load_dates
from load.load_fact_associations import load_associations
import traceback

try:
    # roles = extract("roles")
    # persistir_staging(roles, 'ext_roles')
    # print("Roles persistidos con éxito")
    # user = extract("users")
    # persistir_staging(user, 'ext_users')
    # print("Usuarios persistidos con éxito")
    # companies = extract("companies")
    # persistir_staging(companies, 'ext_companies')
    # print("Empresas persistidas con éxito")
    # associations = extract("associations")
    # persistir_staging(associations, 'ext_associations')
    # print("Asociaciones persistidas con éxito")
    # tra_companies= transform_companies()
    # persistir_staging(tra_companies, "tra_companies")
    # transform_associations =transform_associations()
    # persistir_staging(transform_associations, "tra_associations")
    # load_roles()
    # load_companies()
    # load_users()
    # load_dates()
    load_associations()

   
except:
    traceback.print_exc()
finally:
    pass
