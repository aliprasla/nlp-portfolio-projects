
import requests
import logging
import re
import os
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup as Soup
from urllib.parse import urljoin
from posixpath import join as joindir

BASE_URL= "https://imsdb.com"
MOVIE_SCRIPT_ADDITION_URL = "alphabetical"
# SUB_PAGE_LIST = ["0","A","B", "C", "D", "E", "F", "G", "H",
#                 "I", "J", "K", "L", "M", "N", "O", "P", "Q", 
#                 "R", "S", "T", "U", "V", "W", "X", "Y", "Z" ]

SUB_PAGE_LIST = ["0"]

DATA_DIR_LOCATION = 'data/'
OUTPUT_FILE_NAME = 'raw_script_data.csv'

logging.basicConfig(level="INFO")

class DataScraper:
    def __init__(self):
        self.script_data = None

    # the web page lists all genres on each page. We must eliminate them from our set
    MOVIE_GENRE_FIXED_LIST_INDEX = 18
    def capture_movie_summary_suffix_list(self):
        final_url_list = []
        for suffix in SUB_PAGE_LIST:

            url = urljoin(BASE_URL,joindir(MOVIE_SCRIPT_ADDITION_URL,suffix))
            
            soup = self._load_soup(url)
            movie_url_list = [tag['href'] for tag in soup.select('a[href^="/Movie Scripts/"]')]

            # append to url list
            final_url_list = final_url_list + movie_url_list

        # eliminate duplicates
        return list(set(final_url_list))

    def capture_script_data_from_page(self,movie_summary_url_suffix):

        try: 
            movie_summary_url = urljoin(BASE_URL,movie_summary_url_suffix)

            movie_summary_soup = self._load_soup(movie_summary_url)



            # extract movie writers

            writer_list = [ writer_name_elem.text for writer_name_elem in movie_summary_soup.select('a[href^="/writer.php?"]')]

            # extract genres

            unclean_genre_list = [i.text for i in movie_summary_soup.select('a[href^="/genre"]')]

            clean_genre_list = unclean_genre_list[self.MOVIE_GENRE_FIXED_LIST_INDEX:]
            
            
            # extract script date

            unclean_script_date = movie_summary_soup(text = re.compile(" : "))[-1]
            clean_script_date = unclean_script_date.lstrip(" : ")


            # get script url -
            temp_script_url_list = movie_summary_soup.select('a[href^="/scripts"]')

            # check to see if there are two elements
            assert len(temp_script_url_list) == 2, "Unexpected number of script elements"
            
            script_url = urljoin(BASE_URL, temp_script_url_list[0]['href'])

            
            # extract movie title

            unclean_title = script_url.replace('https://imsdb.com/scripts/','')

            movie_title = unclean_title.rstrip('.html')



            script_soup = self._load_soup(script_url)

            # this will require some data cleaning later
            raw_script = script_soup.find('td',{'class':'scrtext'})

        except Exception as e:
            logging.error(f"Something went wrong. Error: {e} Opening Debuggin shell.")
            import code
            code.interact(local=locals())    


        return {
            'movie_title': movie_title,
            'collected_at': np.datetime64('now'),
            'movie_summary_url': movie_summary_url,
            'movie_script_url': script_url,
            'script_writers': writer_list,
            'genre':clean_genre_list,
            'script_date': clean_script_date,
            'script_text': raw_script

        }


    def _load_soup(self, url):
        """
        Makes GET request and loads soup object after validating
        """

        logging.info(f"Pulling {url}")

        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"Request to pull {url} failed. Status code: {response.status_code}")

        return Soup(response.text,'html.parser')

    def extract(self):
        # first, capture all 
        logging.info("Beginning Data Extraction on {BASE_URL}")
        logging.info("Beginning Hyperlink List")
        
        suffix_list = self.capture_movie_summary_suffix_list()

        all_script_data = [self.capture_script_data_from_page(suffix) for suffix in suffix_list]
        
        self.script_data = all_script_data

    def save_script_data_to_csv(self):
        if not os.path.exists(DATA_DIR_LOCATION):
            os.mkdir(DATA_DIR_LOCATION)

        if self.script_data is None:
            raise ValueError("You must call the .extract method before saving script data")

        final_file_name = os.path.join(DATA_DIR_LOCATION,OUTPUT_FILE_NAME)
        self.script_data.to_csv(final_file_name)
        



if __name__ == "__main__":
    extractor = DataScraper()
    extractor.extract()
    extractor.save_script_data_to_csv()
