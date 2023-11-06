'''

Aaron Chen
aaron.chen@spc.ox.ac.uk

Company House Officers Data Person Classifier

Take Company House scraped data and classify individuals to with the same first and last name from individual's date of birth and registered address (postal code). 
Return a combined dataframe with an extra column 'person classification' with numbers assigned to all entries with the same name. Entries with same name and same person classification number indicate that they are the same person. Unsure entries (no date of birth and address) are marked with a '*' in the column. 


'''



import networkx as nx
import pandas as pd
import numpy as np
from tqdm import tqdm
import glob
import json
import matplotlib.pyplot as plt
import re
from random import randint
from multiprocessing import Pool, cpu_count
import operator


def person_classifier(row, persons_dict, person_number): 
    if row['DOB'] != '0-0':
        if row['DOB'] in persons_dict:
            return persons_dict[row['DOB']]
        else:
            persons_dict[row['DOB']] = person_number[0]
            persons_dict[row['postal_code']] = person_number[0]
            person_number[0] += 1
            return persons_dict[row['DOB']]
    elif row['postal_code'] != '0':
        if row['postal_code'] in persons_dict:
            return persons_dict[row['postal_code']]
        else:
            persons_dict[row['postal_code']] = person_number[0]
            person_number[0] += 1
            return persons_dict[row['postal_code']]
    return '*'


def worker_function(names_chunk, officer_data_DF):
	output = pd.DataFrame()
	if len(names_chunk) == 0: 
		return output

	for name in names_chunk:
		entries = officer_data_DF[officer_data_DF['name'] == name].copy()
		persons_dict = {}
		person_number = [1]
		entries['person_classification'] = entries.apply(lambda row: person_classifier(row, persons_dict, person_number), axis=1)
		output = pd.concat([output, entries])
	return output


def name_classifier(officer_data_DF): 
    names = list(set(officer_data_DF['name'].values))
    # Split the names into chunks, one for each CPU core
    num_cores = cpu_count()
    names_chunks = [names[i::num_cores] for i in range(num_cores)]
    
    with Pool(num_cores) as pool:
        results = pool.starmap(worker_function, [(chunk, officer_data_DF) for chunk in names_chunks])
        
    for r in results: 
    	print(f'dataframe length : {len(r)}')
    # Concatenate the results from all the workers
    output = pd.concat(results)
    print(f'output dataframe length : {len(output)}')
    return output


def process_row(row, Names, CompanyDict):
    if row['name'] in Names:
        return CompanyDict[row['name']], True
    elif re.search('\w{2}\d{6}', str(row['name'])):
        return row['name'], True
    elif re.search('(?i)LIMITED|LTD', str(row['name'])): 
    	return row['name'], True
    else:
        return row['name'], False


def main():
	merge_1_2 = pd.read_csv('merge_1_2_monthly.csv', dtype = str)
	merge_3_5_6 = pd.read_csv('merge_3_5_6_monthly.csv', dtype = str)
	merge_4 = pd.read_csv('merge_4_monthly.csv', dtype = str)
	merge_7_12 = pd.read_csv('merge_7_12_monthly.csv', dtype = str)
	merge = pd.concat([merge_1_2, merge_3_5_6, merge_4, merge_7_12])
	# merge = pd.concat([merge_1_2, merge_4])
	del merge_1_2, merge_3_5_6, merge_7_12, merge_4



	merge.drop_duplicates(subset=['name', 'company_id', 'type'], keep='last', inplace=True)
	merge.drop_duplicates(subset=['name', 'company_id', 'DOB'], keep='last', inplace=True)
	merge.drop_duplicates(subset=['name', 'company_id', 'postal_code'], keep='last', inplace=True)
	merge.reset_index(drop=True, inplace=True)



	CompanyNumberName = pd.read_csv('CompanyNumberName.csv')
	CompanyDict = CompanyNumberName.set_index('CompanyName')['CompanyNumber'].to_dict()
	Names = set(CompanyDict.keys())

	results = merge.apply(lambda row: process_row(row, Names, CompanyDict), axis=1)
	merge.loc[:, 'name'], comp_link = zip(*results)

	comp_link = list(comp_link)
	pers_link = np.logical_not(comp_link)
	merge_comp = merge[comp_link]
	merge_pers = merge[pers_link]
	print(f'merge length {len(merge)}; comp_link {len(merge_comp)}; pers_link {len(merge_pers)}')
	# assert len(merge_comp) + len(merge_pers) == len(merge), f'merge length {len(merge)} is not the sum of comp_link {len(merge_comp)} and pers_link {len(merge_pers)}'

	no_dup = merge_pers.drop_duplicates(subset=['name'], keep=False, inplace=False)
	no_dup.reset_index(drop=True, inplace=True)
	good_set = pd.concat([merge_comp, no_dup])
	# print(len(good_set))
	duplicates = merge_pers[merge_pers.duplicated(['name'], keep=False)]
	duplicates.reset_index(drop=True, inplace=True)
	print(f'good_set length : {len(good_set)}')
	print(f'merge_pers length {len(merge_pers)}; duplicates {len(duplicates)};  no_dup {len(no_dup)}')
	# assert len(duplicates) + len(no_dup) == len(merge_pers), f'merge_pers length {len(merge_pers)} is not the sum of duplicates {len(duplicates)} and no_dup {len(no_dup)}'


	print('Start Processing （っ＾▿＾)っ!')
	dups = name_classifier(duplicates)
	print(f'duplicates length : {len(duplicates)} ; ####### output length : {len(dups)}')

	# dups['name'] = dups['name'] + ' ' + dups['person_classification'].astype(str)
	fixed_dup_set = dups[['name', 'relation', 'kind', 'company_id', 'start', 'end', 'postal_code', 'DOB', 'type', 'person_classification']]

	output_set = pd.concat([good_set, fixed_dup_set])
	# print(len(output_set))
	output_set.to_csv('FINAL_OUTPUT_MONTHLY.csv', index = False)
	print(f'OUTPUT LENGTH : {len(output_set)}, INPUT LENGTH : {len(merge)}')
	# assert len(output_set) == len(merge), f"Input output DF length not the same! with difference in length : {len(output_set) - len(merge)}"
	print(f'difference in length : {len(output_set) - len(merge)}')
	

	# dups['person_classification'] = pd.to_numeric(dups['person_classification'], errors='coerce')

	# name_check = set(dups[dups['person_classification'] > 1].name.values)
	# for name in name_check: 
	# 	print(dups[dups['name'] == name].to_string())


if __name__ == '__main__': 
	main()
