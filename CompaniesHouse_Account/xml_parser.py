'''

Aaron Chen
aaron.chen@spc.ox.ac.uk

XML Parser

Contain functions to clean and collect xml file data from Companies House Account Data Files. Returns four dataframes for year 2018,2019,2020,2021. 
'''

import os
import re
import glob
import os
from tqdm import tqdm
import httpx
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import json
from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS 
import multiprocessing
import concurrent.futures


def mapping(date): 
    avail_year = set(['2018', '2019', '2020', '2021'])
    year = date.split('-')[0]
    if year not in avail_year: 
        year = 'before'
    return year

def clean_value(string):
	"""
	Take a value that's stored as a string,
	clean it and convert to numeric.
	
	If it's just a dash, it's taken to mean
	zero.
	"""
	if string.strip() == "-":
		return(0.0)
	
	try:
		return(float(string.strip().replace(",", "").replace(" ", "")))
	except:
		pass
	
	return(string)


def retrieve_unit(soup, element): 
	try: 
		unit_str = element.attrs['unitRef']
	except:
		return("NA")

	return(unit_str.strip())

def retrieve_date(soup, element): 
	try:
		date_info = element.attrs['contextRef'][1:]
		date = parser.parse(date_info).date().isoformat()
		return(date)
	except: 
		pass

	return("NA")

def retrieve_from_context(soup, contextref):
	"""
	Used where an element of the document contained no data, only a
	reference to a context element.
	Finds the relevant context element and retrieves the relevant data.
	
	Returns a text string
	
	Keyword arguments:
	soup -- BeautifulSoup souped html/xml object
	contextref -- the id of the context element to be raided
	"""
	
	try:
		context = soup.find("xbrli:context", id=contextref)
		contents = context.find("xbrldi:explicitmember").get_text().split(":")[-1].strip()
		
	except:
		contents = ""
	
	return(contents)

def parse_element(soup, element):

	# Only taking relevant blocks
	if "contextRef" not in element.attrs: 
		return({})


	element_dict = {}

	# parse name block
	try: 
		element_dict['name'] = element.name.lower().split(":")[-1]
	except: 
		pass

	element_dict['value'] = element.get_text()
	element_dict['unit'] = retrieve_unit(soup, element)
	element_dict['date'] = retrieve_date(soup, element)

	if element_dict['value'] == "":
		element_dict['value'] = retrieve_from_context(soup, element.attrs['contextref'])

	if element_dict['unit'] != "NA":
		element_dict['value'] = clean_value(element_dict['value'])

	try:
		element_dict['sign'] = element.attrs['sign']
		
		# if it's negative, convert the value then and there
		if element_dict['sign'].strip() == "-":
			element_dict['value'] = 0.0 - element_dict['value']
	except:
		pass


	return(element_dict)

def parse_elements(element_set, soup): 
	elements = []
	for each in element_set: 
		element_dict = parse_element(soup, each)
		if 'name' in element_dict: 
			elements.append(element_dict)
	return(elements)

def scrape_elements(soup, filepath): 
	try: 
		element_set = soup.find_all()
		elements = parse_elements(element_set, soup)
		if len(elements) <= 5: 
			raise Exception("Elements should be gte 5, was {}".format(len(elements)))
		return(elements)
	except: 
		pass 

	return(0)



def process_account(filepath):
	doc = {}
	doc['doc_name'] = filepath.split("/")[-1]
	doc['doc_type'] = filepath.split(".")[-1].lower()

	sheetdates = filepath.split("/")[-1].split(".")[0].split("_")[-1]
	doc['doc_balancesheetdate'] = datetime.strptime(sheetdates, "%Y%m%d").date().isoformat()

	doc['doc_companieshouseregisterednumber'] = filepath.split("/")[-1].split(".")[0].split("_")[-2]

	try: 
		soup = BS(open(filepath, 'r'), 'xml')
	except: 
		print("Failed to open" + filepath)
		return(1)

	try: 
		doc['elements'] = scrape_elements(soup, filepath)
	except Exception as e:
		doc['Error'] = e

	try: 
		return(doc)
	except Exception as e: 
		return(e)

def read_account_Data(file): 
    account_data_2021 = {}
    account_data_2020 = {}
    account_data_2019 = {}
    account_data_2018 = {}

    # try getting the first file (an XML, or XBRL, file)
    doc = process_account(file)
    # print(doc['elements'])

    # display for fun
    
    #display(pd.DataFrame(doc)['doc_companieshouseregisterednumber'].unique()[0])
    #display(pd.DataFrame(doc['elements']))
    company_number = pd.DataFrame(doc)['doc_companieshouseregisterednumber'].unique()[0]
    df_company_finances = pd.DataFrame(list(doc['elements']))


    

    try: 
        company_name_all = df_company_finances[df_company_finances['name']=='entitycurrentlegalname'].value.unique()
        company_name = company_name_all[0]
    except: 
        company_name = 'no_name'
        print(company_number + ' has no name')
        no_name_companies.append(company_number)
        
    if len(company_name_all) > 1: 
        print(company_number + ' has multiple name: ' + company_name_all)
    report_cover_date = df_company_finances[df_company_finances['name']=='balancesheetdate'].value.unique()[0]
    #province = df_company_finances[df_company_finances['name']=='principallocation-cityortown'].value.unique()[0]
    totalassetslesscurrentliabilities = df_company_finances[df_company_finances['name'] == 'netassetsliabilitiesincludingpensionassetliability']
    
    # change date to datetime object to compare
    # totalassetslesscurrentliabilities['date'] = pd.to_datetime(totalassetslesscurrentliabilities['date'])
    totalassetslesscurrentliabilities.loc[:,'date'] = totalassetslesscurrentliabilities['date'].map(mapping)
    
    if len(totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == 'before']) >= 1: 
        account_before_2018.append([company_name, company_number, file])
        

    dormant = df_company_finances[df_company_finances['name'] == 'companydormant'].value.iloc[0]
    section_480 = df_company_finances[df_company_finances['name'] == 'companyentitledtoexemptionundersection480companiesact2006'].value.iloc[0]


    try:
        TALCL_2021 = totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == '2021'].value.iloc[0]
    except: 
        TALCL_2021 = None        
        
    try:
        TALCL_2020 = totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == '2020'].value.iloc[0]
    except: 
        TALCL_2020 = None
        
    try:
        TALCL_2019 = totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == '2019'].value.iloc[0]
    except: 
        TALCL_2019 = None

    try:
        TALCL_2018 = totalassetslesscurrentliabilities[totalassetslesscurrentliabilities['date'] == '2018'].value.iloc[0]
    except: 
        TALCL_2018 = None
        
    
    if TALCL_2021: 
        account_data_2021['CompanyNumber'] = company_number
        account_data_2021['CompanyName'] = company_name
        #account_data_2021['Location'] = province
        account_data_2021['totalassetslesscurrentliabilities'] = TALCL_2021
        account_data_2021['CompanyDormant'] = dormant
        account_data_2021['section_480'] = section_480
        #final_2021.append(account_data_2021)
        
    if TALCL_2020: 
        account_data_2020['CompanyNumber'] = company_number
        account_data_2020['CompanyName'] = company_name
        #account_data_2020['Location'] = province
        account_data_2020['totalassetslesscurrentliabilities'] = TALCL_2020
        account_data_2020['CompanyDormant'] = dormant
        account_data_2020['section_480'] = section_480
        #final_2020.append(account_data_2020)
    
    if TALCL_2019: 
        account_data_2019['CompanyNumber'] = company_number
        account_data_2019['CompanyName'] = company_name
        #account_data_2019['Location'] = province
        account_data_2019['totalassetslesscurrentliabilities'] = TALCL_2019
        account_data_2019['CompanyDormant'] = dormant
        account_data_2019['section_480'] = section_480
        #final_2019.append(account_data_2019)
    
    if TALCL_2018: 
        account_data_2018['CompanyNumber'] = company_number
        account_data_2018['CompanyName'] = company_name
        #account_data_2018['Location'] = province
        account_data_2018['totalassetslesscurrentliabilities'] = TALCL_2018
        account_data_2018['CompanyDormant'] = dormant
        account_data_2018['section_480'] = section_480
        #final_2018.append(account_data_2018)

    return [account_data_2018, account_data_2019, account_data_2020, account_data_2021]
    

# Please Change the directory here! 
files_1 = glob.glob('Accounts_Monthly_Data-January2022/*.xml')
files_2 = glob.glob('Accounts_Monthly_Data-February2022/*.xml')
files_4 = glob.glob('Accounts_Monthly_Data-April2022/*.xml')
files = files_1
files.extend(files_2)
files.extend(files_4)
files = list(set(files))
final_2021 = []
final_2020 = []
final_2019 = []
final_2018 = []
account_before_2018 = []
no_name_companies = []

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

    print("Number of files parsed: {}".format(len(files)))

    print("Length of the DataFrame: {}; Number of Dormant Companies: {}".format(len(df_2020), df_2020[df_2020['CompanyDormant']=='true']['CompanyDormant'].count()))
    print("Number of Active Companies (2020) encoded in xml files: {}".format(df_2020[df_2020['CompanyDormant']=='false']['CompanyDormant'].count()))
    print("Total Value in 2020 Accounts encoded in xml files: {}".format(df_2020['totalassetslesscurrentliabilities'].sum()))
    print("Length of the DataFrame: {}; Number of Dormant Companies: {}".format(len(df_2021), df_2021[df_2021['CompanyDormant']=='true']['CompanyDormant'].count()))
    print("Number of Active Companies (2021) encoded in xml files: {}".format(df_2021[df_2021['CompanyDormant']=='false']['CompanyDormant'].count()))
    print("Total Value in 2021 Accounts encoded in xml files: {}".format(df_2021['totalassetslesscurrentliabilities'].sum()))
    
    # value_dist = df_2021['totalassetslesscurrentliabilities']
    # plt.figure(figsize=(15,9))
    # plt.hist(value_dist, bins=1000)
    # plt.show()

    print(df_2021.sort_values('totalassetslesscurrentliabilities', ascending=False)[['CompanyNumber', 'totalassetslesscurrentliabilities']][:20])


    month = 3
    # Please Name Based on the month of the data 
    # Naming Convention: account_20**_1.csv for Account_Monthly_Data-January
    df_2018.to_csv('account_2018_test_xml{}.csv'.format(month), index=False)
    df_2019.to_csv('account_2019_test_xml4.csv', index=False)
    df_2020.to_csv('account_2020_test_xml4.csv', index=False)
    df_2021.to_csv('account_2021_test_xml4.csv', index=False)




