import asyncio
import time

import aiohttp
from bs4 import BeautifulSoup as bs
import pandas as pd

from scraper_utils import agent, BASE_URL, URL


async def get_pagination_num(url: str) -> int:
    """
    Coroutine for extracting the number
    of last page.
    Args:
        - url.
    Return number as integer.
    """
    async with aiohttp.ClientSession(headers=agent()) as session:
        async with session.get(url) as response:
            text = await response.text()
            html = bs(text, 'lxml')
            lst = html.find('li', class_='current').text.split()
            number = list(filter(lambda x: x.isdigit(), lst))[-1]
            return int(number)


async def fetcher(session: aiohttp.ClientSession, url: str) -> list:
    """
    Coroutine that generate information
    about each book from a page.
    Args:
        - session;
        - url.
    Return list object.
    """
    async with session.get(url) as response:
        text = await response.text()
        html = bs(text, 'lxml')
        items = html.find_all('article', class_='product_pod')
        result = [
            {
            'title': item.img['alt'],
            'link': BASE_URL + item.a['href'],
            'price': float(item.find('p', class_='price_color').text[1:])
        }
        for item in items
        ]
        return result


async def parser(number: int) -> list:
    """
    Main coroutine that parse all pages
    and create final books list.
    Args:
        - number;
    Return list object.
    """
    async with aiohttp.ClientSession(headers=agent()) as session:
        urls = (URL.format(x) for x in range(1, number + 1))
        tasks = [fetcher(session, url) for url in urls]
        return await asyncio.gather(*tasks)


start = time.perf_counter()
number = asyncio.run(get_pagination_num(BASE_URL))
result = asyncio.run(parser(number))
lst = [x for sub_lst in result for x in sub_lst]
exucution_time = time.perf_counter() - start

# form and save csv file
df = pd.DataFrame(lst)
df.to_csv('books.csv')

# info about parser work
print(f'Parse {BASE_URL} executed successfully!!!')
print('[+] Executed time ~', exucution_time, 'seconds!')
print(f'[+] {number} pages parsed!')
print(f'[+] Information about {str(df.shape[0])} books was saved into books.csv file!')
