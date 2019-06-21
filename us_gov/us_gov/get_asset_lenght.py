#%%
import requests
import sys
# /home/jumana/ocn/ocn/us_gov/us_gov/main_metadata.py
from us_gov.us_gov.main_metadata import data
# from asset_scraping.un_scraper.un_scraper.scrapy_results.base_json import base_json
from urllib3.exceptions import ProtocolError

#%%
import logging
logger = logging.getLogger()
logger.handlers = []

# Set Level
logger.setLevel(logging.DEBUG)

# Create Formatter
FORMAT = "%(asctime)s %(levelno)s - %(module)-15s - %(funcName)-15s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(FORMAT, DATE_FMT)

# Create handler and assign
handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(formatter)
logger.handlers = [handler]
logger.critical("logging started")

#%%
# import the raw list with dictionaries (=> scrapy result: base_json)
url_lists = []

def urls_to_list():
    for i in data:
        element = i["files"]

        url_lists.append(element)

    print(len(url_lists))
    return url_lists

#%%
# EXECUTE FUNCTION
urls_to_list()
#%%

all_asset_sizes_list = []

def get_asset_lengths(url_list):
    # get each list in url_list
    for list in url_list:

        # create new list for dataset sizes for one asset
        single_size_list = []

        for item in list:
            try:
                req = requests.get(item)

                content_size = req.headers["Content-Length"]
                content_gb = int(content_size)

                single_size_list.append(content_gb)

                logging.info("got filsize : {} MB for asset {}".format(content_gb, item))
            except KeyError:
                single_size_list.append("0")
                continue
            except requests.exceptions.SSLError:
                single_size_list.append("0")
                continue
            except requests.exceptions.ChunkedEncodingError:
                single_size_list.append("0")
                continue
            except requests.exceptions.ConnectionError:
                single_size_list.append("0")
                continue
            except ProtocolError:
                single_size_list.append("0")
                continue
            except OSError:
                single_size_list.append("0")
                continue
        print(single_size_list)
        all_asset_sizes_list.append(single_size_list)
    print(len(all_asset_sizes_list))
    print(all_asset_sizes_list)
#%%
# EXECUTE FUNCTION
get_asset_lengths(url_lists)