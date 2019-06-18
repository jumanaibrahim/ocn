# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

#scraped data -> item containers -> json/csv files
#scraped data -> item containers -> pipeline -> SQL database

class DataportalScrapePipeline(object):
    def process_item(self, item, spider):
        return item
