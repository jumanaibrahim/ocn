import bs4 as bs #importing beautiful soup for html parsing
import urllib.request
import re
import logging
import json
#create logger and set its level to DEBUG
logger = logging.getLogger('data_crawl')
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

file_to_open = "/home/jumana/ocn/ocn/us_gov/businessUSA_links.csv"

#opening the file that contains all the links with the datasets that need scraping
urlFile = open(file_to_open, "r") #this is the file created by the spider in links.py
# urlFile = open("/home/jumana/ocn/ocn/us_gov/test_links.csv", "r") #this is the file created by the spider in links.py
logger.debug("Opening file {}".format(urlFile))

#empty list which will store all the dataset metadata
base_json=[]

#this builds the metadata for each dataset
def BuildDict(url):
	sauce = urllib.request.urlopen(url) #opens the website for beautiful soup
	soup = bs.BeautifulSoup(sauce, 'lxml')
	dict = {} #temporary dictionary for each individual url-data storage
	links = [] #holds all rawlinks
	tags=[] #holds all tags
	rawLink = soup.find_all("a", attrs={'data-format': 'csv'}) #finds all the csv rawlinks
	tagList = soup.find_all('a',{'class': 'tag'}) #finds all the tags
	categories = "Interdisciplinary"#the category you are currently scraping, change this once you switch between categories

	#adds rawlinks to the 'links' list.
	for i in range(0, len(rawLink)):
		links.append(rawLink[i]['href'])
	#adds tags to the 'tags' list
	for i in range(0,len(tagList)):
		tags.append((tagList[i].text.strip()))

#find a way to do the same without these multiply try and except statements !!!!
	try:
		dict['name'] = soup.find("h1", attrs={'itemprop': 'name'}).text.strip()
		dict['dateCreated'] = soup.find('th', text=re.compile('Metadata Created Date')).parent.find('td').text.strip()
		dict['price'] = 0
		dict['checksum'] = 0
		dict['categories'] = categories
		dict['_tags'] = tags # post on slack --> check what tags are allowed
		dict['type'] = 'dataset'
		dict['inLanguage'] = 'en'
		dict['workExample'] = ''
		dict['files'] = links
	except:
		pass

	try: #finds the author under the two common tags used for it
		dict['author'] = soup.find('th', text=re.compile('Publisher')).parent.find('td').text.strip()
	except:
		try:
			dict['author'] = soup.find('th', text=re.compile('Responsible Party')).parent.find('td').text.strip()
		except:
			pass

	try: #scrapes license data, and if it cannot be found appends 404
		dict['license'] = soup.find('th', text=re.compile('License')).parent.find('td').text.strip()
	except:
		dict['license'] = "404"

	try: #if no description is available, leaves it blank
		dict['description'] = soup.find('div', attrs={'itemprop': 'description'}).text.strip()
	except:
		dict['description'] = ''

	#puts the author as the copyright holder
	dict['copyrightHolder'] = dict['author']
	dict['index'] = dict['name']

	#appends the dictionary into the main metadata list
	base_json.append(dict)
	logger.debug("Finished appending dictionary for : {}".format(dict['name']))


md_filename= 'businessUSA_data' #name of the metadata file
#function which writes all the metadata captured into a file called main_metadata
def buildFile():
	# with open('main_metadata.txt', 'w') as file:
	with open(md_filename+'.txt', 'w') as file:
		file.write(json.dumps(base_json))
		logger.debug("Writing to file".format())

#MAIN PROGRAM WHICH RUNS THE FUNCTIONS
#calls to each url in the list, builds its dictionary and appends to base_json
for url in urlFile:
	logger.debug("Building dictionary for:  {}".format(url.strip()))
	BuildDict(url.strip())

#finished building a .txt file with all the metadata
buildFile()