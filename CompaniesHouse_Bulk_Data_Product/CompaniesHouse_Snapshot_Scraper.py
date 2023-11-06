'''

Aaron Chen
aaron.chen@spc.ox.ac.uk

Companies House Bulk Data Product Prod195_3500 scraper. Returns a dataframe (.csv) file of the all information.



'''

import pandas as pd
from tqdm import tqdm
import glob


def header(line): 
    header_dict = {'DDDDSNAP':'snapshot_file',
                  'DDDDUPDT':'update_file'}
    header_indicator = line[:8]
    run_number = line[8:12]
    production_date = line[12:20]
    
    return header_dict[header_indicator], run_number, production_date

def trailer(line): 
    record_count = int(line.strip()[8:16])
    
    return record_count

def company(line): 
    company_status_dict = {'C':'Converted/closed company', 
                          'D':'Dissolved company', 
                          'L':'Company in liquidation', 
                          'R':'Company in receivership', 
                          ' ':'Else'}
    company_number = line[:8]
    record_type = line[8:9]
    if record_type != '1':
        raise Exception(f'record type {record_type} picked up as company line!')
    company_status = company_status_dict[line[9:10]]
    num_officers = int(line[32:36])
    
    company_name_len = int(line[36:40])
    
    company_name = line[40:40+company_name_len-1]
    
    return company_name, company_number, company_status, num_officers

def person(line, i): 
    appointment_dict = {'00':'Current Secretary',
                       '01':'Current Director',
                       '04':'Current non-designated LLP Member',
                       '05':'Current designated LLP Member',
                       '11':'Current Judicial Factor',
                       '12':'Current Receiver or Manager appointed under the Charities Act', 
                       '13':'Current Manager appointed under the CAICE Act', 
                       '17':'Current SE Member of Administrative Organ',
                       '18':'Current SE Member of Supervisory Organ', 
                       '19':'Current SE Member of Management Organ'}
    company_number = line[:8]
    
    record_type = line[8:9]
    
    if record_type != '2':
        raise Exception(f'record type {record_type} picked up as person line!')
    
    # Not useful - document origin of the appointment date
    app_date_origin = line[9:10]
    
    appointment_type = line[10:12]
    try: 
        appointment = appointment_dict[appointment_type]
    except: 
        appointment = appointment_type
        print(f'New appointment type : {appointment_type} not included in the dictionary!')
        
    person_number = line[12:24]
    
    corp_indicator = line[24:25]
    if corp_indicator == 'Y': 
        is_corp = True
    else: 
        is_corp = False
    
    appointment_date = line[32:40].strip()
    if not appointment_date: 
        appointment_date = None
    
    resignation_date = line[40:48].strip()
    if not resignation_date: 
        resignation_date = None
    
    person_postcode = line[48:56].strip()
    if not person_postcode: 
        person_postcode = None
    
    if len(line[64:72].strip()) != 0: 
        DOB = line[64:72].strip()
    else: 
        DOB = line[56:64].strip()
        
    if not DOB: 
        DOB = None
        
    data_len = int(line[72:76])
    
    person_info_str = line[76:76+data_len].split('<')
    person_info_categories = ['title','forenames','surname','honours','care of','po box',\
                              'address line 1','address line 2','post town','county','country',\
                              'occupation','nationality','usual residential country','']
#     if i == 2074396: 
#         person_info_str = person_info_str[1:]
        
    if len(person_info_str) == 16: 
        person_info_str = person_info_str[1:]
    
    assert len(person_info_str) == len(person_info_categories), f"Length of variable {len(person_info_str)} \
    info not equal to predefined categories length {len(person_info_categories)}! \n In Line {int(i)+1}! "
    person_info = { person_info_categories[i]:(person_info_str[i] if person_info_str[i] \
                   else None) for i in range(len(person_info_str)-1) }
#     person_info['fullname'] = person_info['surname'] + ', ' + person_info['forenames'].capitalize()
    
    
    return company_number, appointment, person_number, is_corp, appointment_date, person_postcode, DOB, person_info
    
    
def main():
    total_list = list()
    files_path = glob.glob('Prod195_3500/*')
    for data in tqdm(files_path[:5]):
        with open(data, 'r') as file: 
            officers_details = []
            officer = {}
            for i, line in enumerate(file): 
                
                # if i >= 100: 
                #     break
                
                if i == 0: 
                    file_info, run_number, production_date = header(line)
                    continue
                
                if line[:8] == '99999999': 
                    record_count = trailer(line)
                    break
                
                if line[8] == '1':
                    officer = {}
                    officer['company_name'], officer['company_number'], officer['company_status'], \
                    officer['num_officers'] = company(line)
                    continue
                
                if line[8] == '2': 
                    company_number, appointment, person_number, is_corp, \
                    appointment_date, person_postcode, DOB, person_info = person(line, i)
                    
                    if company_number == officer['company_number']: 
                        officer_1 = officer.copy()
                        officer_1.update(person_info)
                        officer_1['person_number'], officer_1['is_corp'], \
                        officer_1['appointment'], officer_1['appointment_date'],\
                        officer_1['DOB'], officer_1['person_postcode'] = person_number, is_corp, appointment,\
                        appointment_date, DOB, person_postcode
                        
                        officers_details.append(officer_1)
                    else: 
                        raise Exception("Person from another company recorded in the wrong officer dictionary")
                    
                    continue

            total_list.extend(officers_details)

    total_df = pd.DataFrame(total_list)

    return total_df





if __name__ == '__main__': 
	total_df = main()
	print(len(total_df))
	total_df.to_csv('total_DF.csv', index=False)