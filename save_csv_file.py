import numpy as np
import os
import glob
import pandas as pd
from datetime import datetime
import datetime as td
import time
st1 = time.time()
import threading
# ///////// Find New Production Plan ///////////
# Set the folder path
st = time.time()
folder_path = "C:\\Users\\keshe\Documents\\MAS\\Excel sheet ganerator"

# Get a list of all Excel files in the folder
excel_files = glob.glob(os.path.join(folder_path, 'Vaanavil Production Plan *.xlsx'))

# Extract the dates from the file names
dates = [os.path.splitext(os.path.basename(file))[0].split('Vaanavil Production Plan ')[1].replace(' - ', '') for file in excel_files]
# change the str Dtype to dtats tyepe
dates = [datetime.strptime(date_str, '%m-%d-%Y').date() for date_str in dates]
# Maximamum date
max_date = max(dates)
# change the Dates Dtype to str type
max_date = max_date.strftime('%Y-%m-%d')

Loding_date = (f'{max_date[5:7]}-{max_date[8:]}-{max_date[:4]}')
file_name = os.path.join(folder_path, f'Vaanavil Production Plan {max_date[5:7]}-{max_date[8:]}-{max_date[:4]}.xlsx')
file_name_Production = file_name.split ('\\')
print(file_name_Production[-1])
print('today ;- ', Loding_date)

# ///////// END Find New Production Plan //////////////


# ``````````````Start New Autonomation solution data base ``````````

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
print(file_name_Autonomation[-1])

# ``````````````End New Autonomation solution data base ``````````
print('Befor_vanavil_loding_plan:-',time.time()-st)
st = time.time()

# file_name = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\Vaanavil Production Plan 02-01-2023.xlsx'
# Autonomation_file = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\Autonomation solution data base 01 13 23.xlsx'

vanavil_loding_plan = pd.read_excel(file_name, "Loading Plan")
print('vanavil_loding_plan:-',time.time()-st)
st = time.time()

auto_lodingplan = vanavil_loding_plan[['Module', 'Style', 'Strip Planned', 'PSD','PED','Planned Qty']]  #'Module', 'Style', 'Strip Planned', 'PSD','PED' get colums
auto_lodingplan = auto_lodingplan[auto_lodingplan['Module'].str.contains('Daily Prod|Efficiency') == False] # Remove str (Daily Prod,Efficiency) contaion rows
auto_lodingplan.insert(2, 'Unick_ID', auto_lodingplan.apply(lambda x: '%s%s' % (x['Module'], x['Style']), axis=1) ) #Create by unic_ID with Module&Style to


auto_lodingplan = auto_lodingplan.dropna (subset= ['Planned Qty']) #Delete Empty Planned Quantity 
auto_lodingplan.drop(columns= 'Planned Qty', inplace= True) #Delete Planned Quantity
auto_lodingplan = auto_lodingplan[auto_lodingplan['Strip Planned'].apply(lambda x: str(x).isdigit())] #Remove emty strip planned 

Date_format = pd.DataFrame()
Date_format['PED'] = pd.to_datetime(auto_lodingplan['PED']).dt.date #PSD Data format change datetime to date format
Date_format['PSD'] = pd.to_datetime(auto_lodingplan['PSD']).dt.date #PED Data format change datetime to date format

auto_lodingplan['PSD'] = Date_format['PSD'] #Copy PSD Date_format to auto_lodingplan
auto_lodingplan['PED'] = Date_format['PED'] #Copy PED Date_format to auto_lodingplan


Date_max_Min = pd.DataFrame()
Date_max_Min['PSD'] = auto_lodingplan.groupby ('Unick_ID') ['PSD'].min() # get Min Date for the Unick_ID
Date_max_Min['PED'] = auto_lodingplan.groupby ('Unick_ID') ['PED'].max() # get Max Date for the Unick_ID
Date_max_Min.reset_index(inplace=True) #reset Index for oder

sum_strip = pd.DataFrame()
sum_strip['Strip Planned'] = auto_lodingplan.groupby('Unick_ID')['Strip Planned'].sum() #Sum total strip planned by unick_id
sum_strip.reset_index(inplace=True) # Unick_ID Index column to move 1st Column


sum_strip = pd.merge(sum_strip, auto_lodingplan[['Unick_ID', 'Module','Style']], on= 'Unick_ID', how= 'left')    #merge uni_ID to module style
sum_strip = sum_strip.drop_duplicates(subset=['Unick_ID']) #Remove duplicate Unick_ID
sum_strip.reset_index(drop=True, inplace= True) #Restart index and remove old index

sum_strip['PSD'] = Date_max_Min['PSD'] #PSD copy to sum_strip
sum_strip['PED'] = Date_max_Min['PED']  ##PED copy to sum_strip

col2 = sum_strip.pop('Strip Planned') #popup strip planned column
sum_strip.insert(3, 'Strip Planned', col2) #popup column insert to 3rd index column
sum_strip = sum_strip.sort_values(['Module', 'PSD']) #sort values 1st psd then module vaise
sum_strip.drop(columns= 'Unick_ID', inplace= True) #remove Unick_ID
print('Abow_ASL:-',time.time()-st)
st = time.time()
Auto_SC = pd.read_excel( Autonomation_file, 'KREEDA')
Auto_SC['Solution Code'] = Auto_SC['Production'].apply(lambda x: ','.join([i for i in x.split(',') if i != 'USP' and i != '']))
print('Blow_ASL:-',time.time()-st)
st = time.time()

sum_strip = pd.merge(sum_strip, Auto_SC[['Style', 'Solution Code']], on= 'Style', how='left')
sum_strip = sum_strip.replace(np.nan, '', regex=True)

Loding_date = datetime.strptime(Loding_date, '%m-%d-%Y').date()
week_number = str(Loding_date.isocalendar()[1])
if len(week_number)<2:
    week_number= '0'+week_number
    pass
tmp_parth = 'C:\\Users\\keshe\\Documents\\MAS\\Excel sheet ganerator\\Autonomation production base solustion list {}.xlsx'.format('Week - '+week_number) 

print(tmp_parth)
sum_strip.to_excel(tmp_parth, index=False, sheet_name= f'Production_ASL {Loding_date}')
print('final:-',time.time()-st)
print('total :- ', time.time()-st1)

# with pd.ExcelWriter(tmp_parth, engine='openpyxl', mode='a') as writer:
#     sum_strip.to_excel(writer, sheet_name='Production Auto-SL', index= False)


