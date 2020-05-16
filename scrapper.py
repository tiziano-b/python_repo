from string import punctuation

#MYSQLSTUFF
import Database.DataModel as model
import re
import mysql.connector

#TWITTER STUFF
import tweepy
import json

#deal with API limitation
import time

#STOCK STUFF - http request
import requests
# -- save in csv and transport in MySQL
import csv
import pandas as pd
import os


class Database_connector:
    cur = None
    connection = None

    def __init__(self):
        with open('database.json') as cred_data:
            info = json.load(cred_data)
            host_ = info['HOST_ADDRESS']
            user_ = info['USER_ID']
            password_ = info['PASSWORD']
            db_ = info['DB_CODE']
        print("ciao")
        host = host_
        user = user_
        password = password_
        db = db_

        cnx = mysql.connector.connect(host=host,
                user=user,
                password=password,database=db)
        self.connection = cnx
        self.cur = cnx.cursor()
        print("connected")

    def tweet_populator(self, tweet):
        try:

            # Remove hyperlinks
            tweet_cleaned = re.sub(r'https?:\/\/.*\/\w*', '', tweet.content)
            tweet_cleaned = tweet_cleaned.replace('\n', ' ')
            tweet_cleaned = re.sub(r'[' + punctuation.replace('@', '') + ']+', ' ', tweet_cleaned)
            #INSERT IGNORE = skipp tweet with same key (duplicate)
            sql = f"INSERT IGNORE INTO tweets VALUES ({tweet.id},\"{tweet.name}\",\"{tweet.date}\",{tweet.followers},{tweet.friends},{tweet.retweet},\"{tweet_cleaned}\")"
            self.cur.execute(sql)
            # connection is not autocommit by default. So you must commit to save your changes.
            self.connection.commit()
        finally:
            pass

    def stock_price_daily_populator(self, stock_price_daily):
        try:
            stock_price_daily.print()
            NAME = stock_price_daily.name
            TIMESTAMP = stock_price_daily.timestamp
            OPEN = stock_price_daily.open
            HIGH = stock_price_daily.high
            LOW = stock_price_daily.low
            CLOSE = stock_price_daily.close
            VOLUME = stock_price_daily.volume
            # Create a new record

            sql = f"INSERT IGNORE INTO stock_prices VALUES (\"{NAME}\",\"{TIMESTAMP}\",{OPEN},{HIGH},{LOW},{CLOSE},{VOLUME})"

            self.cur.execute(sql)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.connection.commit()
        finally:
            pass

    def stock_price_intraday_populator(self, stock_price_daily):
        try:
            stock_price_daily.print()
            NAME = stock_price_daily.name
            TIMESTAMP = stock_price_daily.timestamp
            OPEN = stock_price_daily.open
            HIGH = stock_price_daily.high
            LOW = stock_price_daily.low
            CLOSE = stock_price_daily.close
            VOLUME = stock_price_daily.volume
            # Create a new record

            sql = f"INSERT IGNORE INTO stock_prices_intraday VALUES (\"{NAME}\",\"{TIMESTAMP}\",{OPEN},{HIGH},{LOW},{CLOSE},{VOLUME})"

            self.cur.execute(sql)

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.connection.commit()
        finally:
            # close the connection
            # self.connection.close()
            pass


class Twitter_scrapper:
    def crawl_tweets_cashtag(self, cashtag, maximum_number_of_tweets_to_be_extracted, database):


        with open('twitter_credentials.json') as cred_data:
            info = json.load(cred_data)
            consumer_key = info['CONSUMER_KEY']
            consumer_secret = info['CONSUMER_SECRET']
            access_key = info['ACCESS_KEY']
            access_secret = info['ACCESS_SECRET']


        # Create the api endpoint
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        cursor = tweepy.Cursor(api.search, q='$' + cashtag, count=100, tweet_mode='extended').items(maximum_number_of_tweets_to_be_extracted)

        while True:
            try:
                tweet = cursor.next()

                if 'retweeted_status' in dir(tweet):
                    scrapped_tweet = model.Tweet(tweet.id,cashtag,tweet.created_at,tweet.user.followers_count,tweet.user.friends_count,tweet.retweet_count,tweet.retweeted_status.full_text)
                    database.tweet_populator(scrapped_tweet)
                    # print("-FUULL RETWEET")
                    # print(tweet.retweeted_status.full_text)
                    # print("-FUULL TEXT")
                    # print(tweet.full_text)

                # Insert into db
                else:
                    scrapped_tweet = model.Tweet(tweet.id,cashtag,tweet.created_at,tweet.user.followers_count,tweet.user.friends_count,tweet.retweet_count,tweet.full_text)
                    database.tweet_populator(scrapped_tweet)
                    #print("-FUULL RETWEET -- in no retweet")
                    #print(tweet.retweeted_status.full_text)
                    # print("-FUULL TEXT -- in no retweet")
                    # print(tweet.full_text)

                time.sleep(0)
            except tweepy.TweepError:
                print("EXCEPTION TIME TWITTER API - wait 15mins")
                time.sleep(60 * 15)
                continue
            except StopIteration:
                break

        print ('Extracted ' + str(maximum_number_of_tweets_to_be_extracted) \
               + ' tweets with cashtag $' + cashtag)


class Stock_price_scrapper:


    def download_newStockPrice(self, hashtag, database):
        startTime = time.time()
        print("~ deal with API limitation: 5 calls per minute")
        for i in range(0, 15):
            print(i)
            # making delay for 1 second
            time.sleep(1)
        endTime = time.time()
        elapsedTime = endTime - startTime
        print("Elapsed Time = %s" % elapsedTime)

        with open('twitter_credentials.json') as cred_data:
            info = json.load(cred_data)
            API_KEY1 = info['alphavantage']

        # request CSV keyName
        request_key = requests.get(
            'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + hashtag + '&apikey=' + API_KEY1 + '&datatype=csv')

        if (request_key.status_code == 200):
            # print (r.json())
            for r in request_key:
                print(r)
            new_path = 'new_Values_' + hashtag + '.csv'
            new_values = open(new_path, 'w')
            writer = csv.writer(new_values)
            reader = csv.reader(request_key.text.splitlines())
            for row in reader:
                writer.writerow(row)
            new_values.close()
            list_onlyDate,list_onlyOpen, list_onlyHigh, list_onlyLow, list_onlyClose, list_onlyVolume = self.read_stockprices_panda_csv(new_path)

            # DATABASE UPDATE -----------------------------
            for index in range(0, len(list_onlyDate) - 1):
                stock_price_daily = model.StockPrice_Daily(hashtag, list_onlyDate[index], list_onlyOpen[index],
                                                           list_onlyHigh[index], list_onlyLow[index],
                                                           list_onlyClose[index], list_onlyVolume[index])
                database.stock_price_daily_populator(stock_price_daily)

            os.remove(new_path)


    def download_newStockPrice_intraday(self, hashtag, database):
        startTime = time.time()
        print("~ deal with API limitation: 5 calls per minute")
        for i in range(0, 15):
            print(i)
            # making delay for 1 second
            time.sleep(1)
        endTime = time.time()
        elapsedTime = endTime - startTime
        print("Elapsed Time = %s" % elapsedTime)

        with open('twitter_credentials.json') as cred_data:
            info = json.load(cred_data)
            API_KEY1 = info['alphavantage']

        #request CSV keyName
        request_key = requests.get(
            'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+hashtag+'&interval=5min&apikey='+API_KEY1 +'&datatype=csv')

        if (request_key.status_code == 200):
            for r in request_key:
                print(r)
            new_path = 'new_Values_' + hashtag + '.csv'
            new_values = open(new_path, 'w')
            writer = csv.writer(new_values)
            reader = csv.reader(request_key.text.splitlines())
            for row in reader:
                writer.writerow(row)
            new_values.close()
            list_onlyDate,list_onlyOpen, list_onlyHigh, list_onlyLow, list_onlyClose, list_onlyVolume = self.read_stockprices_panda_csv(new_path)

            # DATABASE UPDATE -----------------------------
            for index in range(0, len(list_onlyDate) - 1):
                stock_price_daily = model.StockPrice_Daily(hashtag, list_onlyDate[index], list_onlyOpen[index],
                                                           list_onlyHigh[index], list_onlyLow[index],
                                                           list_onlyClose[index], list_onlyVolume[index])
                database.stock_price_intraday_populator(stock_price_daily)

            os.remove(new_path)


if __name__ == '__main__':

    CRAWL_STOCK_DAILY= True
    CRAWL_STOCK_INTRADAY= False
    CRAWL_TWITTER = True

    database = Database_connector()

    stock_nasdaq_top100 = ['ATVI', 'ADBE', 'AMD', 'ALGN', 'ALXN','AMZN','AMGN', 'AAL','ADI','AAPL','AMAT','ASML',
                           'ADSK','ADP','AVGO','BIDU','BIIB','BMRN','CDNS','CELG','CERN','CHKP','CHTR','CTRP','CTAS',
                           'CSCO','CTXS','CMCSA','COST','CSX','CTSH','DLTR','EA','EBAY','EXPE','FAST','FB','FISV',
                           'GILD','GOOG','GOOGL','HAS','HSIC','ILMN','INCY','INTC','INTU','ISRG','IDXX','JBHT','JD',
                           'KLAC','KHC','LRCX','LBTYA','LBTYK','LULU','MELI','MAR','MCHP','MDLZ','MNST','MSFT','MU',
                           'MXIM','MYL','NTAP','NFLX','NTES','NVDA','NXPI','ORLY','PAYX','PCAR','BKNG','PYPL','PEP','QCOM',
                           'REGN','ROST','SIRI','SWKS','SBUX','SYMC','SNPS','TTWO','TSLA','TXN','TMUS','ULTA','UAL','VRSN',
                           'VRSK','VRTX','WBA','WDC','WDAY','WYNN','XEL','XLNX']


    if CRAWL_TWITTER:
        twitter_scrapper = Twitter_scrapper()
        for hash in stock_nasdaq_top100:
            print(hash," is processing")
            twitter_scrapper.crawl_tweets_cashtag(hash, 1400,database)

    if CRAWL_STOCK_DAILY:
        daily_scrapper = Stock_price_scrapper()
        index = 0
        for hash in stock_nasdaq_top100:
            index = index + 1
            print(hash, " is processing - DAILY %d of 100" % index)
            daily_scrapper.download_newStockPrice(hash, database)


    if CRAWL_STOCK_INTRADAY:
        daily_scrapper = Stock_price_scrapper()
        index = 0
        for hash in stock_nasdaq_top100:
            index = index +1
            print(hash, " is processing - INTRADAY %d of 100" %index)
            daily_scrapper.download_newStockPrice_intraday(hash, database)

    exit()
