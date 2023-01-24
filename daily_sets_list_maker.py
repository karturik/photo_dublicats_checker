import os
import pandas as pd
import openpyxl

full_df = pd.DataFrame()
full_df_for_db = pd.DataFrame()

# MERGE ALL ETHNOCITY TABLES TO ONE
for root, subdirectories, files in os.walk('descriptors/credentials/table'):
    for file in files:
        if '.xlsx' in file and not 'sets' in file:
            print(file)
            path = os.path.join(root, file)
            if not 'retry' in file:
                nation = file.split("_")[2]
            else:
                nation = file.split("_")[3]
            if 'Arab' in nation:
                nation = 'Middle East'
            if not 'retry' in file:
                day = file.split("_")[4]
            df = pd.read_excel(path, sheet_name='Sheet1')
            df_for_db = df
            df_for_db['ethnicity'] = nation
            df = df[['link', 'assignment_id']]
            df['ethnicity'] = nation
            full_df = pd.concat([full_df, df])
            full_df_for_db = pd.concat([full_df_for_db, df_for_db])

full_df.to_excel(f'descriptors/credentials/table/sets_for_January_{day}', index=False)
full_df_for_db.to_excel(f'descriptors/credentials/table/db_sets_for_January_{day}', index=False)