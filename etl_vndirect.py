import argparse
import getpass
import glob
import itertools
import os
import time
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from subprocess import PIPE, Popen

import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from pytz import timezone
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import config
import sql_queries
from proxy import get_proxy, return_proxy
from utils import get_logger


def delete_files(path, wildcard, pattern):
    for file in glob.glob(os.path.join(path, wildcard)):
        if pattern in file:
            __logger__.info(f'Deleting file {file}')
            os.remove(file)


def confirm_download(driver):
    """
    Repeatedly check the downloading file completion
    to execute the next iteration
    """
    if not driver.current_url.startswith('chrome://downloads'):
        driver.get('chrome://downloads/')
    return driver.execute_script('''
        var items = downloads.Manager.get().items_;
        if (items.every(e => e.state === 'COMPLETE'))
            return items.map(e => e.file_url);
        ''')


def initialize():
    # Virtual display is used for VPS
    display = None
    if config.use_virtual_screen:
        display = Display(visible=0, size=(1200, 600))
        display.start()

    # Init chrome driver
    url = 'https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml'

    driver = return_proxy()
    driver.get(url)
    return display, driver


def quit(display, driver):
    driver.close()
    driver.quit()
    if config.use_virtual_screen:
        display.popen.terminate()


def web_control(driver, ticker_code, from_date, to_date):
    """ Selenium task to down load the price list """
    def element(css_string):
        elem = WebDriverWait(driver, 60, 1).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_string)), f'Could not find {css_string} for {ticker_code}')
        return elem

    def is_number(s):
        """ Returns True is string is a number """
        a = True if (s.replace('.', '').isdigit() == True) or (
            s.replace(',', '').isdigit() == True) else False
        return a

    try:
        # Input ticker code
        elem = element('#symbolID')
        elem.send_keys(ticker_code)

        # Input time from
        elem = element('#fHistoricalPrice_FromDate')
        elem.send_keys(from_date)

        # Input time to
        elem = element('#fHistoricalPrice_ToDate')
        elem.send_keys(to_date)

        # View historical price list
        elem = element('#fHistoricalPrice_View')
        elem.click()

        if __mode__ == 'first_load':
            ## Download via download button ##
            # Wait until the table appear
            __logger__.info(
                f'Downloading historical price for {ticker_code} from {from_date} to {to_date}')
            elem = element(
                '#tab-1 > div.box_content_tktt > ul')

            # Click download button
            elem = element(
                '#tab-1 > div.box_content_tktt > div > div > a > span.text')
            elem.click()

            # Wait until the file is downloaded successfully by Chrome
            WebDriverWait(driver, 20, 1).until(confirm_download,
                                               f'Could not download for {ticker_code}')
        elif __mode__ == 'incremental_load':
            ## Scrape data via html table ##
            # Wait until the table appear
            __logger__.info(
                f'Downloading historical price for {ticker_code} from {from_date} to {to_date}')
            elem = element(
                '#tab-1 > div.box_content_tktt > ul > li:nth-child(2) > div.row2 > span')

            elem = element('#tab-1 > div.box_content_tktt > ul')
            price_table = elem.get_attribute('innerHTML')

            data_dict = {}
            source = BeautifulSoup(price_table, 'html.parser')
            # Write to raw html file
            with open(f'{config.chrome_download_path}/historical-price-{ticker_code}.html', 'w', encoding='utf-8') as f:
                f.write(str(source))

            # Parsing date
            days = [datetime.strptime(x.get_text().strip(), '%Y-%m-%d').strftime('%d/%m/%Y')
                    for x in source.select('li div.row-time.noline')[1:]]
            data_dict['ticker'] = [ticker_code for x in range(len(days))]
            data_dict['date'] = days

            # Parsing prices
            prices = [(float(x.get_text().strip().replace(',', '')) if is_number(x.get_text().strip(
            )) else x.get_text().strip()) for x in source.select('li div.row1')]
            data_dict['open'] = prices[6::6]
            data_dict['high'] = prices[7::6]
            data_dict['low'] = prices[8::6]
            data_dict['close'] = prices[9::6]
            data_dict['average'] = prices[10::6]
            data_dict['adjusted'] = prices[11::6]

            # Parsing volume
            volumes = [((float(x.get_text().strip().replace(',', ''))) if is_number(
                x.get_text().strip()) else None) for x in source.select('li div.row3')[2:]]
            data_dict['volume'] = volumes[0::2]

            df = pd.DataFrame(data_dict)
            df.to_csv(
                f'{config.chrome_download_path}/historical-price-{ticker_code}.csv', index=None)

            __logger__.info(f'Completed for {ticker_code}')

    except Exception as e:
        __logger__.error('web_control: ' + ticker_code +
                         ' | ' + getattr(e, 'message', repr(e)))


def process_cleaning():
    """
    Kill the the left-over processes
    This method is brutal and needs to be upgraded later
    """
    kill_chrome = "sudo pkill -f chrome".split()
    kill_xfvb = "sudo pkill -f Xvfb".split()

    # Kill Chrome and Xvfb
    __logger__.info('Killing Chrome and Xvfb processes')
    p = Popen(['sudo', '-S'] + kill_chrome, stdin=PIPE,
              stderr=PIPE, universal_newlines=True)
    p.communicate(__sudo_pwd__ + '\n')[1]
    p = Popen(['sudo', '-S'] + kill_xfvb, stdin=PIPE,
              stderr=PIPE, universal_newlines=True)
    p.communicate(__sudo_pwd__ + '\n')[1]
    p_status = p.wait()
    __logger__.info(p_status)

    # Kill Python processes
    __logger__.info('Killing other Python processes')
    py_ids = [int(line.split()[0]) for line in os.popen(
        'ps -A').readlines() if line.split()[-1] == "python"]
    py_ids_tokill = [id for id in py_ids if id != os.getpid()]
    __logger__.info('Current Python ID {0}'.format(os.getpid()))
    for pid in py_ids_tokill:
        kill_py = "sudo kill {}".format(pid).split()
        p = Popen(['sudo', '-S'] + kill_py, stdin=PIPE,
                  stderr=PIPE, universal_newlines=True)
        p.communicate(__sudo_pwd__ + '\n')[1]
        p_status = p.wait()
        __logger__.info(f'Killed Python process {pid}')


def load_historical_price(download_path, pattern):
    """ Ingest data from csv files """
    file_path_list = []
    for item in os.listdir(download_path):
        if os.path.isfile(os.path.join(download_path, item)):
            file_path_list.append(os.path.join(download_path, item))

    with psycopg2.connect(config.conn_string) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cursor:
            for file_path in file_path_list:
                if pattern in file_path and 'csv' in file_path:
                    __logger__.info(
                        f'Uploading historical price from {file_path}')
                    prices = pd.read_csv(file_path)
                    # Capitalize the column headers to be universal
                    prices.columns = map(str.upper, prices.columns)

                    for _, price in prices.iterrows():
                        try:
                            date = datetime.strptime(
                                price.DATE.strip(), '%d/%m/%Y')
                            close = float(price.CLOSE)
                            ticker = price.TICKER.strip()
                            open = float(price.OPEN)
                            high = float(price.HIGH)
                            low = float(price.LOW)
                            volume = int(price.VOLUME)

                            cursor.execute(
                                sql_queries.upsert_historical_price_table,
                                (date, close, ticker, open, high, low,
                                 volume, close, open, high, low, volume)
                            )
                        except Exception as e:
                            __logger__.error('load_historical_price: ' + price.TICKER + ' | ' +
                                             getattr(e, 'message', repr(e)))


def time_filter(ticker_code):
    """
    Filter the conditions of last updated date for each ticker
    based on
    1- inactive days
    2- current weekday as the stock exchange closes on weekend
    """
    latest_date = pd.read_sql(
        sql_queries.latest_update.format(ticker_code), __conn__)['latest_date'].values[0]

    # If data is older than inactive_days argument days then please use first_load
    inactive_days = -365 if __mode__ == 'first_load' else -__inactive_days__

    # Using the date of week and of year to filter
    today = datetime.now()
    dates = [(today + timedelta(days=i)).strftime('%j')
             for i in range(0 - today.weekday(), 7 - today.weekday())]

    if (latest_date < today.date() + timedelta(days=inactive_days)):
        # Inactive stock
        return None
    elif today.strftime('%j') in dates[-2:]:
        # Weekend period
        if latest_date.strftime('%j') < dates[-3]:
            # Ticker not reach Friday
            __logger__.info(f'Ticker {ticker_code} is valid')
            return ticker_code
        else:
            return None
    elif not today.strftime('%j') in dates[-2:]:
        # Weekdays period
        if latest_date.strftime('%j') < (
                today + timedelta(days=-1)).strftime('%j'):
            # Ticker not reach today
            __logger__.info(f'Ticker {ticker_code} is valid')
            return ticker_code
        elif (latest_date.strftime('%j') == (today + timedelta(days=-1)).strftime('%j')) and (datetime.now().hour > 18):
            # Ticker of yesterday
            # get today data after 7pm
            __logger__.info(f'Ticker {ticker_code} is valid')
            return ticker_code
        else:
            return None
    else:
        return None


def data_scraping(ticker_code):
    time_zone = 'Asia/Saigon'
    date_format = '%d/%m/%Y'

    to_date = (datetime.now(timezone(time_zone)) +
               timedelta(days=1)).strftime(date_format)
    from_date = pd.read_sql(sql_queries.latest_update.format(ticker_code), __conn__)[
        'latest_date'].values[0].strftime(date_format)

    # Apply time filter
    if time_filter(ticker_code) is not None:
        try:
            display, driver = initialize()
            web_control(driver, ticker_code,
                        from_date, to_date)
            quit(display, driver)
        except Exception as e:
            __logger__.error('data_scraping: ' + ticker_code + ' | ' +
                             getattr(e, 'message', repr(e)))


def etl():
    """ ETL process of stock prices from VNDIRECT """

    def chunked_iterable(iterable, size):
        # Chunk function to avoid too long run
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                break
            yield chunk

    ticker_codes = pd.read_sql(
        sql_queries.get_ticker_list, __conn__)['ticker_code'].values.tolist()

    for ticker_codes_chunked in chunked_iterable(ticker_codes, 4*__threads__):

        # Clean Chrome and Xvfb processes to reduce memory pressure
        process_cleaning()

        # Clean any remaining csv, htlm before download
        delete_files(config.chrome_download_path,
                     '*.csv', 'historical-price')
        delete_files(config.chrome_download_path,
                     '*.html', 'historical-price')

        # Run selenium to download csv files with multithreading
        __logger__.info(
            'There are {} tickers to download data'.format(len(ticker_codes_chunked)))
        if len(ticker_codes_chunked) > 0:
            p = ThreadPool(processes=__threads__)
            p.map(data_scraping, ticker_codes_chunked)

        # Update changes if any into historical price table
        load_historical_price(config.chrome_download_path,
                              'historical-price')

        # Clean any remaining csv after download
        delete_files(config.chrome_download_path,
                     '*.csv', 'historical-price')
        delete_files(config.chrome_download_path,
                     '*.html', 'historical-price')


def main(args):
    # Init the logging instance
    global __logger__
    __logger__ = get_logger('./log/app.log')

    global __mode__
    __mode__ = args['mode']

    global __threads__
    __threads__ = int(args['threads'])

    global __inactive_days__
    __inactive_days__ = int(args['inactive_days'])

    global __sudo_pwd__
    __sudo_pwd__ = getpass.getpass("sudo pwd: ")

    # Main ETL flow
    while True:
        global __conn__
        __conn__ = psycopg2.connect(config.conn_string)
        etl()
        time.sleep(60)
        __conn__.close()


if __name__ == '__main__':

    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description="""
        Data ETL tool to collect Vietnam stock data from VNDIRECT
        ---------------------------------------------------------
        Specify the follow arguments based on user's situation:
            - mode:
            * use first_load when the whole database needs to be reloaded from beginning
            * the program will download CSV files from the data vendor to ensure the best quality

            * use incremental_load during regular operation
            * the program will ingest information from HTML to increase loading speed

            - inactive_days: this sets the limit of inactive days for a stock to be 
            * considered as a dead one

            - threads: this sets the number of concurrent threads to ingest data
        """)

    ap.add_argument("-m", "--mode",
                    help="ETL mode: first_load, incremental_load", required=True)

    ap.add_argument("-i", "--inactive_days",
                    help="Inactive days", default=7)

    ap.add_argument("-t", "--threads",
                    help="Number of concurrent threads", default=4)

    args = vars(ap.parse_args())

    main(args)
