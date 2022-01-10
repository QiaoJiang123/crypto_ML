"""

Author: Qiao Jiang
Date: 12/20/2021

About this Script:

This script defines a class to retrieve crypto data from polygon.io and convert the data to data frame format if needed.
In order to use this class, you need to have the api key for Crypto data in polygon.io

"""
import requests
import time
import datetime
import os
from twilio.rest import Client
import numpy as np
import json
from pytz import timezone
import pandas as pd


class PolygonCryptoData:
    """

    This class is to retrieve crypto data from polygon.io, both aggregate or real time
    Both start_date and end_date must be EST.

    ticker is in format X:[cointicker][currency] from Snapshot - All Tickers api. The input only needs cointicker

    """
    def __init__(self,polygon_crypto_api_key):
        
        self.polygon_crypto_api_key = polygon_crypto_api_key
        
    def generate_aggregate_api_url(self,ticker,start_date,end_date,timespan = 'minute',multiplier = 1):
        """
        For aggregate data, the api call can only return mat most 50000 results.
        If more than 50,000 records are needed, there will be multiple urls generated and returned
        Also, aggregate api takes UTC but real time api takes EST.
        
        If end_date is now, convert it to date %Y-%m-%d in EST

        ticker from Snapshot - All Tickers api can be used directly in aggregate api

        """
        if end_date == 'now':
            end_date_datetime = datetime.datetime.now()
            end_date_string = end_date_datetime.strftime('%Y-%m-%d')
        else:
            end_date_string = end_date
            
        start_date_datetime = datetime.datetime.strptime(start_date,'%Y-%m-%d')
        end_date_datetime = datetime.datetime.strptime(end_date_string,'%Y-%m-%d')
        
        # divide the time interval into pieces with at most 30-day gap
        start_date_datetime_list = [start_date_datetime+datetime.timedelta(days = 30*i) for i in range(int(np.ceil((end_date_datetime-start_date_datetime).days/30)))]
        end_date_datetime_list = [x-datetime.timedelta(days=1) for x in start_date_datetime_list[1:]]+[end_date_datetime]
        start_date_string_list = [datetime.datetime.strftime(x,'%Y-%m-%d') for x in start_date_datetime_list]
        end_date_string_list = [datetime.datetime.strftime(x,'%Y-%m-%d') for x in end_date_datetime_list]

        date_pair_string_list = [(start,end) for start,end in zip(start_date_string_list,end_date_string_list)]
        
        url = []
        for date_pair in date_pair_string_list:
            url_temp = ('https://api.polygon.io/v2/aggs/ticker/X:'+ticker+
                   'USD/range/'+str(multiplier)+'/'+timespan+'/'+date_pair[0]+'/'+
                   date_pair[1]+'?adjusted=true&sort=asc&limit=50000&apiKey='+self.polygon_crypto_api_key)
            url.append(url_temp)
        
        return url
    
    def get_trade_data_raw_new(self,ticker,start_date,end_date):
        
        """
        Real time api takes EST in YYYY-MM-DD. But it is okay to pass UTC as long as the order is desc and use timestamp.lt
        To be safe, use UTC since UTC's timestamp is larger than EST. In case polygon.io fix the inconsistency
        """

        if end_date == 'now':
            #tz = timezone('EST')
            end_date_datetime = datetime.datetime.now()
            #end_date_datetime = datetime.datetime.now()
            #end_date_string = end_date_datetime.strftime('%Y-%m-%d')
            #largest_timestamp_string = f'{end_date_datetime.timestamp()*1000000000:.0f}'
        else:
            # if end_date is in YYYY-MM-DD, start with the last trade in that day and search in desc order
            end_date_datetime = datetime.datetime.strptime(end_date,'%Y-%m-%d')+datetime.timedelta(days = 1)-datetime.timedelta(minutes = 1)
            #largest_timestamp_string = f'{end_date_datetime.timestamp()*1000000000:.0f}'
        

        #start_date_datetime = datetime.datetime.strptime(start_date,'%Y-%m-%d')+datetime.timedelta(hours = 5) # This is in UTC
        start_date_datetime = datetime.datetime.strptime(start_date,'%Y-%m-%d')

        number_of_days = (end_date_datetime-start_date_datetime).days

        dates_string = [datetime.datetime.strftime(start_date_datetime+ datetime.timedelta(days = i),'%Y-%m-%d') for i in range(number_of_days+1)]
        result_list = []
        for date_string in dates_string:

            smallest_timestamp_string = datetime.datetime.strptime(date_string,'%Y-%m-%d')
            smallest_timestamp_string =  f'{smallest_timestamp_string.timestamp()*1000000000:.0f}'

            url = 'https://api.polygon.io/vX/trades/X:'+ticker+'-USD?timestamp='+date_string+'&order=desc&limit=50000&sort=timestamp&apiKey='+self.polygon_crypto_api_key
            result = requests.get(url)
            result = json.loads(result.content)
            #result_list.extend(result['results'])
            more_page = True
            while more_page:
                if result['count']==0:
                    break
                elif int(smallest_timestamp_string)>result['results'][-1]['participant_timestamp']:
                    result_list.extend([x for x in result['results'] if x['participant_timestamp']>=int(smallest_timestamp_string)])
                    break
                elif 'next_url' not in result.keys(): 
                    result_list.extend(result['results'])
                    break
                else:
                    result_list.extend(result['results'])
                    next_url = result['next_url']+'&apiKey='+self.polygon_crypto_api_key
                    result = requests.get(next_url)
                    result = json.loads(result.content)
                    more_page = 'next_url' in result.keys()
        return result_list
        
    
    def get_trade_data_dataframe_new(self,ticker,start_date,end_date):
        """
        This function convert the result from get_real_time_data_raw into a dataframe.
        Also, it convert timestamp into datatime
        """
        result_list = self.get_trade_data_raw_new(ticker,start_date,end_date)
        result_df = pd.DataFrame({'exchange':[x['exchange'] for x in result_list],
                          'id':[x['id'] for x in result_list],
                          'participant_timestamp':[x['participant_timestamp'] for x in result_list],
                          'price':[x['price'] for x in result_list],
                          'size':[x['size'] for x in result_list]})



        result_df['participant_timestamp_datetime_EST'] = [(datetime.datetime.fromtimestamp(x/1000000000)-datetime.timedelta(hours=5)).strftime('%Y-%m-%d-%H-%M-%S')+'-'+str(x%1000000000) for x in result_df['participant_timestamp']]

        result_df['participant_timestamp_datetime_EST_nanosecond'] = [x.split('-')[-1] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_second'] = [x.split('-')[-2] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_minute'] = [x.split('-')[-3] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_hour'] = [x.split('-')[-4] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_day'] = [x.split('-')[-5] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_month'] = [x.split('-')[-6] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_year'] = [x.split('-')[-7] for x in result_df['participant_timestamp_datetime_EST']]

        result_df.sort_values(by=['participant_timestamp'],inplace=True)
        result_df.drop_duplicates(inplace = True)
        result_df.reset_index(inplace = True, drop = True)
        return result_df
    
    

    def get_trade_data_raw_old(self,ticker,start_date,end_date):
        if end_date == 'now':
            end_date_datetime = datetime.datetime.now()
            end_date_string = end_date_datetime.strftime('%Y-%m-%d')
            #largest_timestamp_string = f'{end_date_datetime.timestamp()*1000000000:.0f}'
        else:
            end_date_string = end_date
            end_date_datetime = datetime.datetime.strptime(end_date,'%Y-%m-%d')

        start_date_datetime = datetime.datetime.strptime(start_date,'%Y-%m-%d')
        number_of_days = (end_date_datetime-start_date_datetime).days
        dates_string = [datetime.datetime.strftime(start_date_datetime+ datetime.timedelta(days = i),'%Y-%m-%d') for i in range(number_of_days+1)]

        result_list = []
        for date_string in dates_string:
            print(date_string)
            url = 'https://api.polygon.io/v1/historic/crypto/BTC/USD/'+date_string+'?limit=10000&apiKey='+self.polygon_crypto_api_key
            result = requests.get(url)
            result = json.loads(result.content)
            result_list.extend(result['ticks'])
            more_pages = True
            #count = -0
            while more_pages:
                #print(count)
                offset_timestamp = result['ticks'][-1]['t']
                #more_pages = (len(result['ticks'])==10000)
                url = 'https://api.polygon.io/v1/historic/crypto/BTC/USD/'+date_string+'?offset='+str(offset_timestamp)+'&limit=10000&apiKey='+self.polygon_crypto_api_key
                result = requests.get(url)
                result = json.loads(result.content)
                result_list.extend(result['ticks'])
                #count+=1
                more_pages = (len(result['ticks'])==10000)
        return result_list


    def get_trade_data_dataframe_old(self,ticker,start_date,end_date):

        result_list = self.get_trade_data_raw_old(ticker,start_date,end_date)

        result_df = pd.DataFrame({'price':[x['p'] for x in result_list],
                  'size':[x['s'] for x in result_list],
                  'timestamp':[x['t'] for x in result_list],
                  'exchange':[x['x'] for x in result_list]})



        result_df['timestamp_datetime_EST'] = [(datetime.datetime.fromtimestamp(x/1000)-datetime.timedelta(hours=5)).strftime('%Y-%m-%d-%H-%M-%S')+'-'+str(x%1000) for x in result_df['timestamp']]

        result_df['timestamp_datetime_EST_millisecond'] = [x.split('-')[-1] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_second'] = [x.split('-')[-2] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_minute'] = [x.split('-')[-3] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_hour'] = [x.split('-')[-4] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_day'] = [x.split('-')[-5] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_month'] = [x.split('-')[-6] for x in result_df['timestamp_datetime_EST']]
        result_df['timestamp_datetime_EST_year'] = [x.split('-')[-7] for x in result_df['timestamp_datetime_EST']]

        result_df.drop_duplicates(inplace = True)

        result_df.sort_values(by=['timestamp'],inplace=True)

        return result_df


    def get_real_time_data_raw(self,ticker,limit):
        
        """
        This function returns real time data. It is the light version of trade data since it only returns up to 50,000 trades

        """
        end_date_datetime = datetime.datetime.now()
        end_date_string = end_date_datetime.strftime('%Y-%m-%d')
        
        url = 'https://api.polygon.io/vX/trades/X:'+ticker+'-USD?timestamp='+end_date_string+'&order=desc&limit='+str(limit)+'&sort=timestamp&apiKey='+self.polygon_crypto_api_key
        result = requests.get(url)
        result = json.loads(result.content)
        
        return result['results']

    def get_real_time_data_dataframe_new(self,ticker,limit):
        
        """
        This function returns real time data in dataframe. It is the light version of trade data since it only returns up to 50,000 trades

        """

        result_list = self.get_real_time_data_raw(ticker,limit)
        
        result_df = pd.DataFrame({'exchange':[x['exchange'] for x in result_list],
                          'id':[x['id'] for x in result_list],
                          'participant_timestamp':[x['participant_timestamp'] for x in result_list],
                          'price':[x['price'] for x in result_list],
                          'size':[x['size'] for x in result_list]})



        result_df['participant_timestamp_datetime_EST'] = [(datetime.datetime.fromtimestamp(x/1000000000)-datetime.timedelta(hours=5)).strftime('%Y-%m-%d-%H-%M-%S')+'-'+str(x%1000000000) for x in result_df['participant_timestamp']]

        result_df['participant_timestamp_datetime_EST_nanosecond'] = [x.split('-')[-1] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_second'] = [x.split('-')[-2] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_minute'] = [x.split('-')[-3] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_hour'] = [x.split('-')[-4] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_day'] = [x.split('-')[-5] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_month'] = [x.split('-')[-6] for x in result_df['participant_timestamp_datetime_EST']]
        result_df['participant_timestamp_datetime_EST_year'] = [x.split('-')[-7] for x in result_df['participant_timestamp_datetime_EST']]

        result_df.sort_values(by=['participant_timestamp'],inplace=True)
        result_df.reset_index(inplace = True, drop = True)

        return result_df

    def get_aggregate_data_raw(self,ticker,start_date,end_date,timespan = 'minute',multiplier = 1):
        
        """

        The result of this function will not be stored as an instance variable
        It will return the result

        """
        aggregate_api_url = self.generate_aggregate_api_url(ticker,start_date,end_date,timespan,multiplier)
        
        result_list = []
        for url in aggregate_api_url:
            result = requests.get(url)
            result = json.loads(result.content)
            if result['queryCount']!=0:
                result_list.extend(result['results'])
        #self.aggregate_api_result = result_list
        return result_list
        #print(self.generate_aggregate_api_url())

    def get_aggregate_data_dataframe(self,ticker,start_date,end_date,timespan = 'minute',multiplier = 1):

        """

        This function convert the result from get_aggregate_data_raw into a dataframe.
        Also, it convert timestamp into datatime

        """
        
        result_list = self.get_aggregate_data_raw(ticker,start_date,end_date,timespan,multiplier)
        result_df = pd.DataFrame({'v':[x['v'] for x in result_list],
                             'vw':[x['vw'] if 'vw' in x.keys() else x['c'] for x in result_list],
                             'o':[x['o'] for x in result_list],
                             'c':[x['c'] for x in result_list],
                             'h':[x['h'] for x in result_list],
                             'l':[x['l'] for x in result_list],
                             't':[x['t'] for x in result_list],
                             'n':[x['n'] if 'n' in x.keys() else 0 for x in result_list]})
        result_df.columns = ['trading_volume','volume_weighted_price','open_price','close_price','highest_price','lowest_price','unix_msec_timestamp','number_of_transactions']
      
        result_df['datetime_EST'] = [(datetime.datetime.fromtimestamp(x/1000)-datetime.timedelta(hours=5)).strftime('%Y-%m-%d-%H-%M') for x in result_df['unix_msec_timestamp']]

        result_df['datetime_EST_minute'] = [x.split('-')[-1] for x in result_df['datetime_EST']]
        result_df['datetime_EST_hour'] = [x.split('-')[-2] for x in result_df['datetime_EST']]
        result_df['datetime_EST_day'] = [x.split('-')[-3] for x in result_df['datetime_EST']]
        result_df['datetime_EST_month'] = [x.split('-')[-4] for x in result_df['datetime_EST']]
        result_df['datetime_EST_year'] = [x.split('-')[-5] for x in result_df['datetime_EST']]

        result_df['datetime_EST_weekday'] = [(datetime.datetime.fromtimestamp(x/1000)-datetime.timedelta(hours=5)).weekday() for x in result_df['unix_msec_timestamp']]
        result_df.reset_index(inplace = True, drop = True)
        # for weekday:
        # 0 - Monday
        # 1 - Tuesday
        # 2 - Wednesday
        # 3 - Thursday
        # 4 - Friday
        # 5 - Saturday
        # 6 - Sunday
        
        return result_df
    
    def get_all_tickers(self):

        """

        This function returns all tickers in polygon.io.

        """

        url = 'https://api.polygon.io/v2/snapshot/locale/global/markets/crypto/tickers?apiKey='+self.polygon_crypto_api_key
        result = requests.get(url)
        result = json.loads(result.content)

        return [x['ticker'] for x in result['tickers']]

