import pandas as pd
import logging
import os
import pytz
from main_metadata import data #import the file that was created in data_crawl. Make sure this file has been changed from .txt to .py and that the list has been named
from licence_map import license_map

#create logger and set its level to DEBUG
logger = logging.getLogger('create_csv')
logger.setLevel(logging.DEBUG)

#format of outputted log and further formatting of its date
FORMAT = "%(asctime)s : %(name)s : %(funcName)s : %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(FORMAT,DATE_FORMAT)

#create a console handler and set its formatting and level
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)

#add the created handler to logger
logger.addHandler(ch)

#create main dataframe
df = pd.DataFrame.from_dict(data)
logger.debug("Creating pandas dataframe.".format())

#sort the columns in correct order
df = df[['name', 'dateCreated', 'author','license','price','categories','_tags','type','description','copyrightHolder','inLanguage','files']]
#create flag to know which row to delete
df['del_flag'] = 0

#will replace licences with their proper mapping
logger.debug("Mapping the licences with their proper counterparts.".format())
df['license'].replace(license_map, inplace = True)
logger.debug("Finished license mapping. Starting to format the dates.".format())

#format the dates to be in the ISO 8601 format
df['dateCreated'] = pd.to_datetime(df['dateCreated'])
dates = df['dateCreated']
lenght = len(dates)
for i in range(0,lenght):
	x = (dates[i])
	logger.debug("Original date: {}".format(x))
	x = x.to_pydatetime().replace(tzinfo = pytz.utc).isoformat().replace('+00:00', 'Z')
	df.replace(df['dateCreated'][i],x, inplace = True)
	logger.debug("Date has been replaced to : {}".format(df['dateCreated'][i]))

#function which deletes the urls that are broken
def  delete_broken_urls():
	logger.debug("Deleting broken urls".format())
	for count,url_list in enumerate(df['files']): #parsing through the column to find the list which holds all urls
		for inner_count, url in enumerate(url_list): #parsing through each url
			if ('accessType=DOWNLOAD' in url): #check if the url is a tru download link
				logger.debug("{} is OK".format(url))
				pass
			elif (url[-4:]) == '.csv':
				logger.debug("{} is OK".format(url))
				pass
			elif (url[-4:]) == '/csv':
				logger.debug("{} is OK".format(url))
				pass
			elif (url[-4:]) == '.zip':
				logger.debug("{} is OK".format(url))
				pass
			else:
				logger.debug("{} is deleted".format(url))
				df['files'][count][inner_count] = 'BROKEN' #replace url with the word 'BROKEN' and set the delete flag to True
				df.del_flag[count] = 1
	df.drop(df.loc[df['del_flag']==1].index, inplace=True) #deleting the rows which have broken urls
	df.drop(columns=['del_flag'], inplace = True) #deleting the column which holds the flags
	df.reset_index(drop = True, inplace = True) #resetting the indexes of the columns to make up for deleted rows

delete_broken_urls() #run the function

#put the munged data into a csv
df=df.transpose()
file_name = "/dataTrialFinal.csv"
path = os.getcwd()
print(path+file_name)
df.to_csv(path+file_name)