import traceback
import pandas as pd


def extraer_date():
    try:
        filename = './csvs/dates.csv'
        dates = pd.read_csv(filename)   
        return dates
    except:
        traceback.print_exc()
    finally:
        pass