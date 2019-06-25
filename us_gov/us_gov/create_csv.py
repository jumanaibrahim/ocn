import pandas as pd
import logging
# import datetime
import os
import pytz
# import sys
import wget
from urllib.request import urlopen
# from urllib3.exceptions import ProtocolError
from main_metadata import data
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


all_asset_sizes_list = []
destination = '/home/jumana/Desktop/temp_downloads/test.csv'

def get_asset_length(url):
	try:
		site = urlopen(url)
		content_gb = int(site.getheader('Content-Length'))
		logger.debug("got filesize : {} MB for asset {}".format(content_gb, url))
	except:
		wget.download(url, destination)
		content_gb = os.path.getsize(destination)
		logger.debug("Size of downloaded file: {} MB for asset {}".format(content_gb,url))
		os.remove(destination)
		logger.debug("{} has been removed.".format(url))
	return content_gb

def  get_all_lengths():
	for count, url_list in enumerate(df['files']):
		size_list = []
		for inner_count, url in enumerate(url_list):
			size = get_asset_length(url)
			size_list.append(size)
		logger.debug("Appending {} for asset {}".format(size_list,url))
		all_asset_sizes_list.append(size_list)

file_main = {}
def build_files_dict():
	logger.debug("Beginning to build file_main dictionary.".format())
	for count, url_list in enumerate(df['files']):
		file_dict = {}
		for inner_count, url in enumerate(url_list):
			file_dict['file[' + str(inner_count) + ']:url'] = url
			file_dict['file[' + str(inner_count) + ']:index'] = inner_count + 1
			file_dict['file[' + str(inner_count) + ']:contentType'] = 'csv'
			file_dict['file[' + str(inner_count) + ']:checksum'] = 0
			file_dict['file[' + str(inner_count) + ']:checksumType'] = ''
			file_dict['file[' + str(inner_count) + ']:contentLenght'] = all_asset_sizes_list[count][inner_count]
			file_dict['file[' + str(inner_count) + ']:encoding'] = ''
			file_dict['file[' + str(inner_count) + ']:compression'] = ''
			file_dict['file[' + str(inner_count) + ']:resourceId'] = ''
		file_main[str(count)] = file_dict
		logger.debug("Completed appending for asset number: {}".format(count))

def adding_url_info():
	logger.debug("Started adding_url_info.".format())
	for index, inner_dict in file_main.items():
		for key in inner_dict:
			if key not in df:
				df[key] = ''
				df.at[int(index), key] = inner_dict[key]
				logger.debug("Added {} : {}".format(df.at[int(index), key] , inner_dict[key]))
			else:
				df.at[int(index), key] = inner_dict[key]
		logger.debug("Finished formatting for asset : {}".format(index))


delete_broken_urls() #run the functions
get_all_lengths()
build_files_dict()
adding_url_info()

#put the munged data into a csv
df=df.transpose()
file_name = "/earth_and_climate.csv"
path = os.getcwd()
print(path+file_name)
df.to_csv(path+file_name)