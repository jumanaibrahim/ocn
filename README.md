# ocn
#### Documentation and User Manual.

OCN is a scraper to crawl through the US Government's [Open Data](https://www.data.gov/) catalogue

_Let's get you started!_

Open OCN with an IDE of your choice. I prefer using [PyCharm](https://www.jetbrains.com/pycharm/), but it's not necessary to do so. This project scrapes datasets by running three main files: 
1. links.py,
1. data_crawl.py
1. create_csv.py

* ### links.py

#### Set up

Open the following url in your browser: https://catalog.data.gov/dataset. 
Specify both the format and the topic you want to crawl. 

For the purpose of this tutorial, I have chosen the format as __'csv'__ and topic as __'energy'__. 
This narrows our scrape down to about 30 datasets. Let's call the url of [this site](https://catalog.data.gov/dataset?res_format=CSV&groups=energy9485&_vocab_category_all_limit=0&page=1 ) your **base url.**

Open up `links.py` which is in the spiders folder. 
Add your _base url_ as a single item in the `start_urls` list. 

```python
start_urls = ['https://catalog.data.gov/dataset?res_format=CSV&groups=energy9485&_vocab_category_all_limit=0&page=1']
```

#### Scraping

Scroll down to the `def parse(self,response) ` method. 

Edit your **base url** so it doesn't have the trailing '1'.

Add this edited url into the next_page variable. 
```python
next_page ='https://catalog.data.gov/dataset?res_format=CSV&groups=energy9485&_vocab_category_all_limit=0&page='+str(LinkSpider.page_num)
```

###### Optional
Edit the `max_page` variable to better reflect the number of pages you will/want to parse.

#### Running links.py
Run this command in your terminal
``` bash
scrapy crawl mainlinks -o name-of-file.csv
```
I chose to name my file energy_links.csv. 

* Upon its successful completion, a csv file should have popped up in your folder. Open it and you will notice that not only has it scraped all your urls, but it also has the word **'links'** on the first line of the file. Delete this and any other occurrence of the word that you find.

_You're ready to move on to stage 2!_

* ### data_crawl.py

#### Set up
Open up `data_crawl.py`
Set the `name_to_open` variable to the name of the file which holds all your links (the file created by links.py).

Check the `file_to_open` variable to make sure it is pointing towards the correct folder.
My variable and name look something like this:

```python
name_to_open = "energy_links"
file_to_open = "/home/user/ocn/ocn/us_gov/"+name_to_open+".csv"
```

#### Scraping
Read through `def BuildDict()  ` and replace any attributes to correspond to the ones you chose. 
For instance, the topic of 'Energy' belongs to the "Physics & Energy" category, and you have chosen to find csv files. So your code should reflect this:

```python
soup.find_all("a", attrs={'data-format': 'csv'})
categories = "Physics & Energy"
```

#### Running data_crawl.py
Scroll down to the `def buildFile()`. Assign a filename for your metadata file by changing the `md_filename` variable to a name of your choice.

```python
md_filename= 'energy_data'
```
### create_csv.py

#### Set up
Open `create_csv.py`. Import the data dictionary from your `energy_data.py` file. (This is the file created after running data_crawl.py.)

```python
from energy_data import data
```

Set a filepath to a temporary csv file and assign it to the `destination` variable. 

> This is the the temporary file created when downloading your datasets in order to determine their size. These datasets will only be downloaded if "content-lenght" is not included in the header file of the dataset.

```python
destination = '/home/user/Desktop/temp_downloads/test.csv' 
```

Change the `file_name` variable and assign it a name of your choice for your _final_ .csv file.

```python
file_name = "/final_energy_data.csv"
```

##### Run create_csv.py and you're done!
