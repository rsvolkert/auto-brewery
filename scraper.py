import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


class Scraper:
    """ Scrapes from beer forum """
    brewpart_ids = ['fermentables', 'hops', 'others', 'yeasts', 'mashsteps']

    def __init__(self, base, pg_num=1):
        self.soup = self._open_url(base, pg_num)

    @staticmethod
    def _open_url(path, pg_num):
        url = path + str(pg_num)
        req = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
        if req.status_code == 429:
            time.sleep(int(req.headers['Retry-After']))
            req = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
        return BeautifulSoup(req.content, 'html5lib')

    def get_recipe(self, href):
        recipe_base = 'https://www.brewersfriend.com'
        recipe_url = recipe_base + href
        req = requests.get(recipe_url, headers={'User-agent': 'Mozilla/5.0'})
        if req.status_code == 429:
            if req.status_code == 429:
                time.sleep(int(req.headers['Retry-After']))
                req = requests.get(recipe_url, headers={'User-agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(req.content, 'html5lib')

        beer = soup.title.text.split('|')[0].strip()
        stats = {'Name': beer,
                 'ID': int(re.search('view/(.+?)/', href).group(1))}

        for stat in soup.find_all('span', {'class': 'viewStats'}):
            name = stat.find('span').text.strip(':')

            if name == 'Rating':
                stats['Rating'] = float(stat.find('span', {'itemprop': 'ratingValue'}).text)
                stats['Reviews'] = int(stat.find('span', {'itemprop': 'reviewCount'}).text)
            else:
                if stat.find('strong'):
                    entry = stat.find('strong').text
                    stats[name] = entry.strip()

        for brewpart in soup.find_all('div', {'class': 'brewpart'}):
            part_id = brewpart.get('id')

            if part_id in self.brewpart_ids:
                if part_id == 'yeasts':
                    stats['yeasts'] = brewpart.find('th').text.strip()
                else:
                    if brewpart.tfoot:
                        brewpart.tfoot.extract()

                    df = pd.read_html(str(brewpart))[0]
                    df.rename({u'\N{DEGREE SIGN}L'.upper(): 'L'}, inplace=True, axis=1)
                    df_dict = df.transpose().to_dict()

                    stats[part_id] = df_dict
            elif part_id is None:
                if brewpart.find('div', {'class': 'ui message'}):
                    stats['notes'] = brewpart.find('div', {'class': 'ui message'}).text.strip()

        return stats


if __name__ == '__main__':
    import os
    import json
    import pickle
    import pymongo
    from tqdm import tqdm

    base = 'https://www.brewersfriend.com/homebrew-recipes/page/'
    scraper = Scraper(base)

    ul = scraper.soup.find_all('ul', {'class': 'pagination'})
    page_text = ul[1].find_all('li')[0].text.strip()
    page_num = int(page_text.split()[-1].replace(',', ''))

    recipes = []

    # read last page we left off on
    pg = pickle.load(open('pg_idx.txt', 'rb'))
    for pg_idx in tqdm(range(pg+1, page_num+1)):
        # refresh client every page so the connection doesn't go stale
        client = pymongo.MongoClient(f'mongodb+srv://{os.getenv("MONGO_USER")}:{os.getenv("MONGO_PASS")}@'
                                     f'beer-data.9pulebh.mongodb.net/?retryWrites=true&w=majority')
        collection = client['beer-data']['beer-data']

        # get all recipes on the page
        scraper = Scraper(base, pg_idx)
        hrefs = [recipe.get('href') for recipe in scraper.soup.find_all('a', {'class': 'recipetitle'})]

        # open and read each recipe
        for recipe in tqdm(hrefs, leave=False):
            trycnt = 3
            while trycnt > 0:
                try:
                    r = scraper.get_recipe(recipe)

                    # add to DB if we don't already have it
                    if not collection.find_one({'ID': r['ID']}):
                        collection.insert_one(json.loads(json.dumps(r)))

                    trycnt = 0
                except requests.exceptions.ChunkedEncodingError as e:
                    if trycnt <= 0:
                        print(f'Failed to retreived {recipe} in 3 tries')
                    else:
                        trycnt -= 1
                    time.sleep(0.5)

        # write page index
        with open('pg_idx.txt', 'wb') as out:
            pickle.dump(pg_idx, out)

    print('Done')
