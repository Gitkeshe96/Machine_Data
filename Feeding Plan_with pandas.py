import numpy as np
import os
import glob
import pandas as pd
from datetime import datetime
import datetime as td

# ```````````START Feeding Plan File parth```````
# technical_file = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\Feeding Plan-week 5.xlsx'
folder_path = "C:\\Users\\keshe\Documents\\MAS\\Excel sheet ganerator"
excel_files = glob.glob(os.path.join(folder_path, 'Feeding Plan-week *.xlsx'))

Week_Numbers = [os.path.splitext(os.path.basename(file))[0].split('Feeding Plan-week ')[1] for file in excel_files]
Week_Number = max(Week_Numbers)
technical_file = os.path.join(folder_path, f'Feeding Plan-week {Week_Number}.xlsx')

# ```````````END Feeding Plan File parth```````



# ``````````````Start New Autonomation solution data base ``````````
# Autonomation_file = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\Autonomation solution data base 01 13 23.xlsx'

# Set the folder path
folder_path = "C:\\Users\\keshe\Documents\\MAS\\Excel sheet ganerator"
# Get a list of all Excel files in the folder
excel_files = glob.glob(os.path.join(folder_path, 'Autonomation solution data base *.xlsx'))
# Extract the dates from the file names
dates = [os.path.splitext(os.path.basename(file))[0].split('Autonomation solution data base ')[1].replace(' ','-') for file in excel_files]
# change the str Dtype to dtats tyepe
dates = [datetime.strptime(date_str, '%m-%d-%y').date() for date_str in dates]
# Maximamum date
max_date = max(dates)
# change the Dates Dtype to str type
max_date = max_date.strftime('%Y-%m-%d')

Autonomation_file = os.path.join(folder_path, f'Autonomation solution data base {max_date[5:7]} {max_date[8:]} {max_date[2:4]}.xlsx')
file_name_Autonomation = Autonomation_file.split ('\\')
# print(file_name_Autonomation[-1])

# ``````````````End New Autonomation solution data base ``````````

# ``````````````START New Autonomation solution data base ``````````
tmp_parth = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\tmp_V1.xlsx' 
excel_files = glob.glob(os.path.join(folder_path, 'Autonomation production base solustion list Week - *.xlsx'))
Week_Numbers = sorted([os.path.splitext(os.path.basename(file))[0].split('Autonomation production base solustion list Week - ')[1] for file in excel_files])
Week_Number = max(Week_Numbers)
production_ASL = os.path.join(folder_path, f'Autonomation production base solustion list Week - {Week_Number}.xlsx')

# ``````````````End New Autonomation solution data base ``````````

# tmp_parth_tc = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\tmp.xlsx'



# vanavil_loding_file = "C:\\Users\\keshe\Documents\\MAS\\Excel sheet ganerator\\Vaanavil loading plan 08-03-2022.xlsx"

Feeding_plan = pd.read_excel(technical_file,sheet_name='Sheet3') #Get feeding plan worksheet
df = Feeding_plan.drop('TECHNICIAN 01', axis=1) #Drop technician column

# empty_columns = [col for col in df.columns if df[col].isna().all()] #Get Empty columns 
# print(len(empty_columns))
# empty_columns = df.loc[:, df.isnull().all()].columns.tolist() # Get Empty columns 
# print(len(empty_columns))

empty_rows = df.index[df.isnull().all(axis=1)].tolist() # Get Empty rows index

if len(empty_rows) > 0: #if have emty empty rowr then 
    df = df[df.index < empty_rows[0]] #filter only above first empty index rows

df.dropna(axis=1, how='all', inplace=True) #Remove Empty columns 

# df.rename(columns={col: pd.to_datetime(df[col]).dt.date for col in df.columns}, inplace=True)

df = pd.melt(df, value_vars=df.columns, var_name='PSD', value_name='Style-Module')  #colum labele Psd and columns valuse are Style_module
df = df[df['Style-Module'].notnull()] #Remove Null value row

df['PSD'] = pd.to_datetime(df['PSD']).dt.date # PSD tate time formato change date formrt
df.reset_index(drop=True,inplace=True) #reset index

df[['Style', 'Module']] = df['Style-Module'].str.split('_'or'-', expand=True)   #split Style-Module columns style and module
df.insert(1, 'Unick_ID', df.apply(lambda x: '%s%s' % (x['Module'], x['Style']), axis=1)) #creat a unic_ID with style and module

loding_plan = pd.read_excel(production_ASL) # get loding plan datafaram
loding_plan.insert(1, 'Unick_ID', loding_plan.apply(lambda x: '%s%s' % (x['Module'], x['Style']), axis=1)) #creat a unic_ID with style and module
df_1 = pd.merge( df,loding_plan[['Unick_ID', 'Strip Planned']], on= 'Unick_ID', how= 'left') #feeding plan dataFarm inset srip planned from the loding plan

Auto_SC = pd.read_excel( Autonomation_file, 'KREEDA') # Get autonomation solustion database datafam to the auto_ac
Auto_SC['Production'] = Auto_SC['Production'].apply(lambda x: ','.join([i for i in x.split(',') if i != 'USP' and i != ''])) #All solustion split bye ',' and remve USP frome the Auto_SC datafarm

df_2= pd.merge(df, Auto_SC[['Style', 'Production']], on= 'Style', how='left') #inset the SC aout the style vaice
df_1['Solustion Code'] = df_2['Production'] 
df_1 = df_1.replace(np.nan, '', regex=True) # NAN replase Empty sring ''
df_1.drop(['Unick_ID', 'Style-Module'], axis=1,inplace=True) #Remove 'Unick_ID','Style-Module'



sheet_name = 'Feeding_ASC'
# Create a Pandas Excel writer using the existing file
with pd.ExcelWriter(production_ASL, mode='a') as writer:
    # Check if the sheet name already exists in the file
    if sheet_name in writer.book.sheetnames:
        # If the sheet name already exists, delete the old sheet
        idx = writer.book.sheetnames.index(sheet_name)
        writer.book.remove(writer.book.worksheets[idx])
    # Write the new sheet to the file
    df_1.to_excel(writer, sheet_name=sheet_name, index=False)