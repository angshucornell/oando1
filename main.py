import pandas as pd
import os
import json
import copy
import logging
from collections import OrderedDict
import datetime

basePath = os.path.dirname(os.path.abspath(__file__))
heading_df = pd.read_csv(basePath + '/YPSolutions_events/Heading.csv', encoding='iso-8859-1')  
directory_df = pd.read_csv(basePath + '/YPSolutions_events/OnlineMarket.csv', encoding='iso-8859-1')
todays_date = datetime.datetime.now().strftime('%Y-%m-%d')

logging.basicConfig(level=logging.DEBUG)

# function to convert multi level json into single level
def boil_down_array(key, data):
    if type(data) == dict:
        for key, item in data.items():
            yield from boil_down_array(key, item)
    else:
        yield {key:data}

def convert_file(filepath):
    
    logging.debug('Started process for ' + filepath)

    # get unique fields of column desc_meaningful_contact
    with open(filepath) as json_file:
        desc_meaningful_contact_unique = []
        line = json_file.readline()
        counter = 0
        while line != None:
            json_line = json.loads(line)
            if json_line.get('desc_meaningful_contact', None) != None:
                if json_line['desc_meaningful_contact'] not in desc_meaningful_contact_unique:
                    desc_meaningful_contact_unique.append(json_line['desc_meaningful_contact'])
            line = json_file.readline()
            counter += 1
            if counter == 100:
                break

    # create a dictionary out of the unique values
    desc_meaningful_contact_unique_dict = dict((el,0) for el in desc_meaningful_contact_unique)
    logging.debug('Uniqe contact are {}'.format(desc_meaningful_contact_unique_dict))

    # iterate over the entire file and process 
    with open(filepath) as json_file:
        line = json_file.readline()
        counter, master_list, master_count = 0, [], 1
        include_ids = [200137,200139,200298,200300,200302]
        
        while line != None:
            
            json_line, series_list = json.loads(line), OrderedDict()

            # if id not in valid ids, ignore
            if eval(json_line['desc_platform_id']) not in include_ids:
                continue

            for key, items in json_line.items():
                for i in boil_down_array(key, items):
                    series_list.update(i)
            
            # add the previously created dict to our record
            series_list.update(desc_meaningful_contact_unique_dict)
            
            # calculate the number of rows that'll be created after isolating directory and heading
            new_rows_count = 0

            try:
                for directory in json.loads(series_list['lk_directory_heading']):
                    for headings in directory['headings']:
                        new_rows_count += 1
            
            # create new rows based on lk_directory_heading
                for directory in json.loads(series_list['lk_directory_heading']):
                    for headings in directory['headings']:
                        new_series_list = copy.deepcopy(series_list)

                        # implementation of step 6
                        if new_series_list.get('desc_meaningful_contact', None) != None:
                            new_series_list[new_series_list['desc_meaningful_contact']] = 1 / new_rows_count
                        
                        # implementation of step 3 to 5 for heading
                        if not heading_df[heading_df['HeadingEnglishOnlineNo'] == str(int(headings['heading']))].empty:
                            heading_name = heading_df[heading_df['HeadingEnglishOnlineNo'] == str(int(headings['heading']))].iloc[0]
                            new_series_list['Heading_English'] = heading_name['HeadingEnglishOnlineName']
                            new_series_list['Heading_French'] = heading_name['HeadingFrenchOnlineName']
                            new_series_list['Heading_Master'] = heading_name['HeadingEnglishOnlineName'] + '|' + heading_name['HeadingFrenchOnlineName']
                        else:
                            # this condition comes when heading id is not found in mapping 
                            new_series_list['Heading_English'] = headings['heading']
                            new_series_list['Heading_French'] = headings['heading']
                            new_series_list['Heading_Master'] = headings['heading']
                        
                        # implementation of step 3 to 5 for directory
                        if directory['directory'] != 'unknown' and not directory_df[directory_df['OnlineMarketNoYpa'] == str(int(directory['directory']))][directory_df['OnlineMarketInd'] == 'Y'].empty:
                            directory_name = directory_df[directory_df['OnlineMarketNoYpa'] == str(int(directory['directory']))][directory_df['OnlineMarketInd'] == 'Y'].iloc[0]
                            new_series_list['Directory_Name'] = directory_name['OnlineMarketNameRPT']
                        else:
                            # this condition comes when directory id not found in mapping. 
                            new_series_list['Directory_Name'] = directory['directory']

                        master_list.append(new_series_list)
            except:
                pass
                
            line = json_file.readline()
            counter += 1

            logging.debug('Records done {} for file {}. Master count is {}'.format(counter, os.path.basename(filepath), master_count))

            # only work on the first 10000 records
            if counter > 1000:
                df = pd.DataFrame(master_list)
                logging.debug('Creating file ' + filepath[:-5] + '_' + str(master_count) + '.csv')
                df.to_csv(filepath[:-5] + '_' + str(master_count) + '_' + todays_date + '.csv', index=False)
                logging.debug('Created file ' + filepath[:-5] + '_' + str(master_count) + '.csv')
                counter, master_list = 0, []
                master_count += 1

convert_file('D:\\ypsolutions_script\\YPSolutions_events\\tapclicks_YPSolutions_events000000000000\\tapclicks_YPSolutions_events000000000000.json')
