import traceback
import pandas as pd
def extraer_countries():
    try:
        filename = './csvs/countries.csv'
        countries = pd.read_csv(filename)   
        return countries
    except:
        traceback.print_exc()
    finally:
        pass