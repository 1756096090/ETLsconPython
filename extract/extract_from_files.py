import pandas as pd
from pathlib import Path
import traceback

def extract_from_files(csv_path: str, json_path: str):
    try:

        df_csv = pd.read_csv(Path(csv_path))
        df_json = pd.read_json(Path(json_path))
        
        print("Data from CSV:")
        print(df_csv)
        print("\nData from JSON:")
        print(df_json)
        
        
        return df_csv, df_json
        
    except Exception as e:
        print("Error occurred during file extraction:", str(e))
        traceback.print_exc()
        return None, None
