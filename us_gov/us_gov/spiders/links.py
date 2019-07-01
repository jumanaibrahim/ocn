import scrapy
import logging

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

#base link spider that will scrape the catalogue website and return the list of links of the individual datasets.
class LinkSpider (scrapy.Spider):
	name = "mainlinks"
	#EARTH &CLIMATE
	# start_urls = ['https://catalog.data.gov/dataset?res_format=CSV&groups=climate5434&_groups_limit=0&page=1'] #starts at first page
	start_urls = ['https://catalog.data.gov/dataset?res_format=CSV&groups=local&_groups_limit=0&page=1']
	page_num = 2 #page number it will increment to


	def parse(self, response):
		max_page = 6 #maximum number of pages to be scraped
		links = response.css('.dataset-heading a::attr(href)').getall() #gets all the links

		for link in links:
			logger.debug("Scraping and giving out: {}".format(link))

			yield{
				'links': 'https://catalog.data.gov'+link #stores the links
			}

		# next_page ='https://catalog.data.gov/dataset?res_format=CSV&groups=climate5434&_groups_limit=0&page='+str(LinkSpider.page_num) #increments to next page
		next_page ='https://catalog.data.gov/dataset?res_format=CSV&groups=local&_groups_limit=0&page='+str(LinkSpider.page_num) #increments to next page
		logger.debug("Moving on to page number: {}".format(LinkSpider.page_num))

		if LinkSpider.page_num <= max_page:  #only scrapes the first 9 pages.
			LinkSpider.page_num+=1
			yield response.follow(next_page, callback = self.parse)