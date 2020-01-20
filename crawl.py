#!/usr/bin/env python
import os
import json
from pprint import pprint
import logging
import psycopg2
from retry import retry
import pandas as pd
from seleniumwire import webdriver

logging.basicConfig()


@retry(Exception, tries=5, delay=5, backoff=2)
def getBestPrice(driver, formattedDate):
    # Go to the page
    driver.get(f"https://www.decolar.com/shop/flights/search/oneway/RIO/SAO/{formattedDate}/1/0/0/NA/NA/NA/NA/?from=SB&di=1-0")

    # Access requests via the `requests` attribute
    print('These are the requests:')
    pprint(driver.requests)
    matchingRequest = [request for request in driver.requests if
                       request.path.startswith('https://www.decolar.com/shop/flights-busquets/api/v1/web/search')][0]
    driver.wait_for_request(matchingRequest.path, timeout=60)
    print(f'Request to {matchingRequest.path} returned with status code {matchingRequest.response.status_code}.')
    dataClusters = json.loads(matchingRequest.response.body)['clusters']
    bestCluster = list(filter(lambda x: x['bestCluster'] is True, dataClusters))[0]
    return bestCluster['priceDetail']['totalFare']['amount']

@retry(Exception, tries=5, delay=5, backoff=2)
def savePriceToDB(bestPrice, currentDate, date):
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        f"""insert into prices (date, flightdate, departure, arrival, price)
            values ('{pd.to_datetime(currentDate).round('H')}', '{pd.to_datetime(date).date()}', 'RIO', 'SAO', {bestPrice});"""
    )
    conn.commit()
    cur.close()


if __name__ == '__main__':
    currentDate = pd.datetime.today()
    date = currentDate + pd.offsets.Week()
    formattedDate = date.strftime('%Y-%m-%d')

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)
    driver.delete_all_cookies()

    try:
        bestPrice = getBestPrice(driver, formattedDate)
        driver.quit()
    except Exception:
        driver.quit()
        raise

    print(f"The best price is: R$ {bestPrice}")

    savePriceToDB(bestPrice, currentDate, date)
