'''

Aaron Chen 
aaron.chen@spc.ox.ac.uk
03/08/2023

Companies House Account Data Parser (Ver. 1.0.1)

Use the XBRL parser to collect and reorganise important financial metrics by year, returns four DataFrames for 
year 2018, 2019, 2020, and 2021. 

!!! Would be a good idea to comment out the print(filepath) line in the XBRL parser in 'xbrl_import/xbrl_parser.py' (line 434). 
Change account data director in line 309, and output csv file names in line 364-367


Ver. 1.0.1 Update Notes (03/08/2023): 

- Fixed parsing bug when there is only one year of account information 

- Fixed bug that fixed assets are not parsed when they are in other forms
    - Can currently capture: 'propertyplantequipment', 'investmentsfixedassets'
    - Let me know if you have seen other forms of fixed assets that are not included! 

- Fixed bug where liability values are not captured 

- Improved the ability to capture different types of current assets
    - Can currently capture: 'debtors', 'cashbankonhand'
    - Let me know if you have seen other forms of current assets that are not included! 

- Fixed bug where the incorrect equity is captured


'''



import pandas as pd
import networkx as nx
import json
import glob
from tqdm import tqdm
import httpx

from stream_read_xbrl import stream_read_xbrl_zip

from xbrl_import import xbrl_parser as xp
import os
import numpy as np
import pandas as pd
import importlib
import datetime
import multiprocessing
import concurrent.futures




def mapping(date): 
    avail_year = set(['2018', '2019', '2020', '2021'])
    year = date.split('-')[0]
    if year not in avail_year: 
        year = 'before'
    return year

def get_value(df, year):
    try:
        return df[df['date'] == str(year)].value.iloc[-1]
    except:
        return None

def get_liability(cl, ncl, l, year): 
    try: 
        CL = cl[cl['date'] == str(year)].value.iloc[-1]
        NCL = ncl[ncl['date'] == str(year)].value.iloc[-1]
        L = float(CL)+float(NCL)
        return CL, NCL, L
    except: 
        try: 
            L = l[l['date'] == str(year)]
            if len(L) == 2: 
                CL = L.value.iloc[0]
                NCL = L.value.iloc[1]
                L = float(CL) + float(NCL)
                
                return CL, NCL, L

            return L.value.iloc[-1], None, L.value.iloc[-1]
        except: 
            return None, None, None


def get_fixed_asset(fixed_asset, other_fix, year): 
    df1 = fixed_asset[fixed_asset['date'] == str(year)]
    other_df = other_fix[other_fix['date'] == str(year)]
    
    if (len(df1) == 0) & (len(other_df) == 0): 
        return None
    if len(df1) == 0: 
        return other_df.value.sum()
    
    return df1.value.iloc[-1]



def read_account_Data(file): 
    account_data_2021 = {}
    account_data_2020 = {}
    account_data_2019 = {}
    account_data_2018 = {}
    
    
    importlib.reload(xp)

    # try getting the first file (an XML, or XBRL, file)
    doc = xp.process_account(file)

    # display for fun
    
    #display(pd.DataFrame(doc)['doc_companieshouseregisterednumber'].unique()[0])
    #display(pd.DataFrame(doc['elements']))
    company_number = pd.DataFrame(doc)['doc_companieshouseregisterednumber'].unique()[0]
    df_company_finances = pd.DataFrame(list(doc['elements']))
    try: 
        company_name_all = df_company_finances[df_company_finances['name']=='entitycurrentlegalorregisteredname'].value.unique()
        company_name = company_name_all[0]
    except: 
        company_name = 'no_name'
        print(company_number + ' has no name')
        no_name_companies.append(company_number)
        
    if len(company_name_all) > 1: 
        print(company_number + ' has multiple name: ' + company_name_all)
    report_cover_date = df_company_finances[df_company_finances['name']=='balancesheetdate'].value.unique()[0]
    #province = df_company_finances[df_company_finances['name']=='principallocation-cityortown'].value.unique()[0]
    

    # Other forms of fixed assets that are not included in fixed assets
    propertyplant = df_company_finances[df_company_finances['name'] == 'propertyplantequipment'].drop_duplicates()
    investmentsfixedassets = df_company_finances[df_company_finances['name'] == 'investmentsfixedassets'].drop_duplicates()

    other_fix = pd.concat([propertyplant, investmentsfixedassets])

    ##### Need to find a way to add the first two if current assets is not there
    fixed_asset = df_company_finances[df_company_finances['name'] == 'fixedassets'].drop_duplicates()
    


    current_asset = pd.concat([df_company_finances[df_company_finances['name'] == 'debtors'], \
        df_company_finances[df_company_finances['name'] == 'cashbankonhand'], \
        df_company_finances[df_company_finances['name'] == 'currentassets']])



    liability = df_company_finances[df_company_finances['name'] == 'creditors']
    
    totalassetslesscurrentliabilities = df_company_finances[df_company_finances['name'] == 'totalassetslesscurrentliabilities']
    netassets = df_company_finances[df_company_finances['name'] == 'netassetsliabilities']
    
    capital = df_company_finances[df_company_finances['name'] == 'equity'].drop_duplicates()[-2:]
    
    
    other_fix.loc[:, 'date'] = other_fix['date'].map(mapping)
    fixed_asset.loc[:, 'date'] = fixed_asset['date'].map(mapping)
    current_asset.loc[:, 'date'] = current_asset['date'].map(mapping)
    liability.loc[:, 'date'] = liability['date'].map(mapping)
    capital.loc[:, 'date'] = capital['date'].map(mapping)

    totalassetslesscurrentliabilities.loc[:,'date'] = totalassetslesscurrentliabilities['date'].map(mapping)
    netassets.loc[:,'date'] = netassets['date'].map(mapping)
    
    
    if len(liability) >= 4: 
        # if len(liability) != 4: 
        #     print("{} has {} liability entries!!! Please look into the html file with name: {}".format(company_number, len(liability), file))
        liability = liability.loc[:liability.index[0]+15]
        try:
            current_liability = liability[:2]
            non_current_liability = liability[2:4]
        except: 
            print("{} has {} liability entries after +15 steps!!! Please look into the html file with name: {}".format(company_number, len(liability), file))
            current_liability = None
            non_current_liability = None
    else: 
        current_liability = None
        non_current_liability = None
    # change date to datetime object to compare
    # totalassetslesscurrentliabilities['date'] = pd.to_datetime(totalassetslesscurrentliabilities['date'])
    
    
    
    
    
    
    if len(totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == 'before']) >= 1: 
        account_before_2018.append([company_name, company_number, file])
        
    if len(netassets[netassets['date'] == 'before']) >= 1: 
        account_before_2018.append([company_name, company_number, file])
        
    
    
    
    
    
    ########### TALCL
    TALCL_2021 = get_value(totalassetslesscurrentliabilities, 2021)
    TALCL_2020 = get_value(totalassetslesscurrentliabilities, 2020)
    TALCL_2019 = get_value(totalassetslesscurrentliabilities, 2019)
    TALCL_2018 = get_value(totalassetslesscurrentliabilities, 2018)        
    
    ########## NA
    NA_2021 = get_value(netassets, 2021)
    NA_2020 = get_value(netassets, 2020)
    NA_2019 = get_value(netassets, 2019)
    NA_2018 = get_value(netassets, 2018)
        
    ############# Assets      
    FA_2021 = get_fixed_asset(fixed_asset, other_fix, 2021)
    FA_2020 = get_fixed_asset(fixed_asset, other_fix, 2020)
    FA_2019 = get_fixed_asset(fixed_asset, other_fix, 2019)
    FA_2018 = get_fixed_asset(fixed_asset, other_fix, 2018)
    
    CA_2021 = get_value(current_asset, 2021)
    CA_2020 = get_value(current_asset, 2020)
    CA_2019 = get_value(current_asset, 2019)
    CA_2018 = get_value(current_asset, 2018)
    
    ############# Capital
    CAPI_2021 = get_value(capital, 2021)
    CAPI_2020 = get_value(capital, 2020)
    CAPI_2019 = get_value(capital, 2019)
    CAPI_2018 = get_value(capital, 2018)
    
    ################## Liability
    CL_2021, NCL_2021, L_2021 = get_liability(current_liability, non_current_liability, liability, 2021)
    CL_2020, NCL_2020, L_2020 = get_liability(current_liability, non_current_liability, liability, 2020)
    CL_2019, NCL_2019, L_2019 = get_liability(current_liability, non_current_liability, liability, 2019)
    CL_2018, NCL_2018, L_2018 = get_liability(current_liability, non_current_liability, liability, 2018)
        
    
    if TALCL_2021: 
        account_data_2021['CompanyNumber'] = company_number
        account_data_2021['CompanyName'] = company_name
        #account_data_2021['Location'] = province
        account_data_2021['fixedasset'] = FA_2021
        account_data_2021['currentasset'] = CA_2021
        account_data_2021['currentliability'] = CL_2021
        account_data_2021['totalassetslesscurrentliabilities'] = TALCL_2021
        account_data_2021['noncurrentliability'] = NCL_2021
        account_data_2021['netliability'] = L_2021
        account_data_2021['netassetslessnetliability'] = NA_2021
        account_data_2021['equity'] = CAPI_2021


        
        #final_2021.append(account_data_2021)

        
    if TALCL_2020: 
        account_data_2020['CompanyNumber'] = company_number
        account_data_2020['CompanyName'] = company_name
        #account_data_2021['Location'] = province
        account_data_2020['fixedasset'] = FA_2020
        account_data_2020['currentasset'] = CA_2020
        account_data_2020['currentliability'] = CL_2020
        account_data_2020['totalassetslesscurrentliabilities'] = TALCL_2020
        account_data_2020['noncurrentliability'] = NCL_2020
        account_data_2020['netliability'] = L_2020
        account_data_2020['netassetslessnetliability'] = NA_2020
        account_data_2020['equity'] = CAPI_2020
        
        #final_2020.append(account_data_2020)
    
    if TALCL_2019: 
        account_data_2019['CompanyNumber'] = company_number
        account_data_2019['CompanyName'] = company_name
        #account_data_2021['Location'] = province
        account_data_2019['fixedasset'] = FA_2019
        account_data_2019['currentasset'] = CA_2019
        account_data_2019['currentliability'] = CL_2019
        account_data_2019['totalassetslesscurrentliabilities'] = TALCL_2019
        account_data_2019['noncurrentliability'] = NCL_2019
        account_data_2019['netliability'] = L_2019
        account_data_2019['netassetslessnetliability'] = NA_2019
        account_data_2019['equity'] = CAPI_2019
        
        #final_2019.append(account_data_2019)
    
    
    if TALCL_2018: 
        account_data_2018['CompanyNumber'] = company_number
        account_data_2018['CompanyName'] = company_name
        #account_data_2021['Location'] = province
        account_data_2018['fixedasset'] = FA_2018
        account_data_2018['currentasset'] = CA_2018
        account_data_2018['currentliability'] = CL_2018
        account_data_2018['totalassetslesscurrentliabilities'] = TALCL_2018
        account_data_2018['noncurrentliability'] = NCL_2018
        account_data_2018['netliability'] = L_2018
        account_data_2018['netassetslessnetliability'] = NA_2018
        account_data_2018['equity'] = CAPI_2018
        
        #final_2018.append(account_data_2018)
        
    return [account_data_2018, account_data_2019, account_data_2020, account_data_2021]
    

# Please Change the directory here! 
files = glob.glob('Accounts_Monthly_Data-February2022/*.html')

##########
# Can do all accounts in one go
# files_6 = glob.glob('Accounts_Monthly_Data-June/*.html')
# files_7 = glob.glob('Accounts_Monthly_Data-July/*.html')
# files_8 = glob.glob('Accounts_Monthly_Data-August/*.html')
# files_9 = glob.glob('Accounts_Monthly_Data-September/*.html')
# ...
# files = files_6
# files.extend(files_7)
# files.extend(files_8)
# files.extend(files_9)



final_2021 = []
final_2020 = []
final_2019 = []
final_2018 = []
account_before_2018 = []
no_name_companies = []

# Maybe change the num_workers to cpu_count() - 1/2
num_workers = multiprocessing.cpu_count()
def main():
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        
        results = executor.map(read_account_Data, files)

    return results

if __name__ == "__main__":
    print('Start Scraping Account Data')

    results = main()

    for r in results: 
        if r[0]: 
            final_2018.append(r[0])
        if r[1]:
            final_2019.append(r[1])
        if r[2]:
            final_2020.append(r[2])
        if r[3]:
            final_2021.append(r[3])
        
    df_2018 = pd.DataFrame(final_2018)
    df_2019 = pd.DataFrame(final_2019)
    df_2020 = pd.DataFrame(final_2020)
    df_2021 = pd.DataFrame(final_2021)


    # Please Name Based on the month of the data 
    # Naming Convention: account_20**_1.csv for Account_Monthly_Data-January
    df_2018.to_csv('account_2018_2.csv', index=False)
    df_2019.to_csv('account_2019_2.csv', index=False)
    df_2020.to_csv('account_2020_2.csv', index=False)
    df_2021.to_csv('account_2021_2.csv', index=False)






    

