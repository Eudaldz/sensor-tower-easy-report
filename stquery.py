# -*- coding: utf-8 -*-
"""
@author: Edwald
"""
import argparse
import time
import datetime
import os.path
import pandas as pd
import requests
import json

if not os.path.exists('results'):
    os.mkdir('results')

def date(string):
    time.strptime(string, "%Y-%m")
    return string
print()
parser = argparse.ArgumentParser(description='Sensor Tower utility script. Query estimated app downloads and revenue.\nPut your api token inside the token.txt file')
parser.add_argument('--date', type=date, help='yyyy-mm. The month of the required app statistics. Defaults to last month.')
parser.add_argument('--category', default='6014', help='string. The app category')
parser.add_argument('--min-download', default=300000, type=int, help='integer. The minimum number of downloads. Defaults to 300000')
parser.add_argument('--min-contribution', default=0.5, type=float, help='(0,1). The minimum contribution ratio. Defaults to 0.5')
args = parser.parse_args()

MONTH_SALES_URL = 'https://api.sensortower.com/v1/unified/sales_report_estimates_comparison_attributes?'\
                'comparison_attribute=delta&'\
                'time_range=month&'\
                'measure=units&'\
                'device_type=total&'\
                'category={category:}&'\
                'date={date:}&'\
                'end_date={end_date:}&'\
                'country=US&'\
                'limit=2000&'\
                'custom_tags_mode=include_unified_apps&'\
                'auth_token={auth_token:}'

APPS_URL = 'https://api.sensortower.com/v1/unified/apps?'\
                'app_id_type=unified&'\
                'app_ids={app_ids:}&'\
                'auth_token={auth_token:}'

ALLTIME_SALES_URL = 'https://api.sensortower.com/v1/unified/sales_report_estimates?'\
                'app_ids={app_ids:}&'\
                'date_granularity=monthly&'\
                'start_date=2012-01-01&'\
                'end_date={end_date:}&'\
                'countries=US&'\
                'auth_token={auth_token:}'

PUBLISHER_URL = 'https://api.sensortower.com/v1/unified/publishers?'\
                'publisher_id_type=unified&'\
                'publisher_ids={publisher_ids:}&'\
                'auth_token={auth_token:}'

APP_ANDROID_INFO = 'https://api.sensortower.com/v1/android/apps?'\
                'app_ids={app_ids:}&'\
                'country=US&'\
                'auth_token={auth_token}'

APP_IOS_INFO = 'https://api.sensortower.com/v1/ios/apps?'\
                'app_ids={app_ids:}&'\
                'country=US&'\
                'auth_token={auth_token}'
    
def main():
    token = open('token.txt', 'r').read()
    print("token="+token)
    print()
    date = args.date
    if not args.date:
        today = datetime.date.today()
        date = "{year:}-{month:}".format(year=today.year, month=today.month)
    df = generate_table(token, date, args.category, args.min_download, args.min_contribution)
    df.to_csv('results/ST-{date:}.csv'.format(date=date), sep='\t', index=False)
    return


def generate_table(token, date, category, min_download, min_contribution):
    
    final_df = pd.DataFrame(columns=['App Id', 'App Name', 'Publisher Id', 'Publisher Name', 'Date',
                'Absolute (Downloads)', 'Change (Downloads)', 'Cumulative (Downloads)', 'Contribute (Downloads)',
                'iOS Release Date', 'Android Release Date', 'Game Genre', 'Game Sub-genre'])
    
    month_df = get_month_sales(token, date, category, min_download)
    if month_df.empty:
        return final_df
    app_ids = list(month_df['app_id'])
    alltime_df = get_app_alltime_sales(token, date, app_ids)
    app_names = get_app_names(token, app_ids)
    month_df = pd.merge(month_df, app_names, on='app_id')
    month_df = pd.merge(month_df, alltime_df, on='app_id')
    
    def find_genre(x):
        for e in x:
            et = e['custom_tags']
            if 'Game Genre' in et:
                return et['Game Genre']
        for e in x:
            et = e['custom_tags']
            if 'Game Theme' in et:
                return et['Game Theme']
        return 'NaN'

    def find_subgenre(x):
        for e in x:
            et = e['custom_tags']
            if 'Game Sub-genre' in et:
                return et['Game Sub-genre']
        return 'NaN'
    
    def find_android_id(x):
        for e in x:
            et = e['custom_tags']
            if 'Google Play Pass' in et:
                return e['app_id']
        return 'NaN'
    
    def find_ios_id(x):
        for e in x:
            et = e['custom_tags']
            if 'Apple Arcade' in et:
                return e['app_id']
        return 'NaN'

    genre = month_df['entities'].apply(find_genre)
    subgenre = month_df['entities'].apply(find_subgenre)
    #TODO FIND THE ACTUAL RELEASE DATES FOR BOTH DEVICES.
    ios_id = month_df['entities'].apply(find_ios_id)
    android_id = month_df['entities'].apply(find_android_id)
    month_df['ios_id'] = ios_id
    month_df['android_id'] = android_id
    ios_id_list = list( filter(lambda x: x!='NaN', ios_id) )
    android_id_list = list( filter(lambda x: x!='NaN', android_id) )
    ios_release_dates = get_ios_release_dates(token, ios_id_list)
    android_release_dates = get_android_release_dates(token, android_id_list)
    month_df = pd.merge(month_df, ios_release_dates, on='ios_id')
    month_df = pd.merge(month_df, android_release_dates, on='android_id')
    final_df['App Id'] = month_df['app_id']
    final_df['App Name'] = month_df['name']
    final_df['Publisher Id'] = month_df['publisher_id']
    final_df['Publisher Name'] = month_df['publisher_name']
    final_df['Date'] = month_df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    final_df['Absolute (Downloads)'] = month_df['units_absolute']
    final_df['Change (Downloads)'] = month_df['units_delta']
    final_df['Cumulative (Downloads)'] = month_df[['units_cumulative', 'units_absolute']].max(axis=1)
    final_df['Contribute (Downloads)'] = final_df['Absolute (Downloads)'] / final_df['Cumulative (Downloads)']
    final_df['Game Genre'] = genre
    final_df['Game Sub-genre'] = subgenre
    final_df['iOS Release Date'] = month_df['ios_release_date']
    final_df['Android Release Date'] = month_df['android_release_date']

    final_df = final_df.query('`Contribute (Downloads)` >= {value:}'.format(value=min_contribution))


    
    return final_df

def get_month_sales(token, date, category, min_download):
    print('Querying app comparison sales of the month {m:}...'.format(m=date))
    start_date = date+'-01'
    end_date = date+'-31'
    url = MONTH_SALES_URL.format(category=category, date=start_date, end_date=end_date, auth_token=token)
    response = requests.get(url)
    if response:
        df = pd.read_json(response.text)
        df.sort_values(by='units_delta', ascending=False)
        df = df.query('units_absolute >= {value:}'.format(value=min_download))
        print("Done.")
        return df
    else:
        print(response.text)
        return pd.DataFrame()

def get_app_alltime_sales(token, date, app_ids):
    print('Querying app sales of all time...')
    date = date+'-31'
    app_id_list = str(app_ids).replace(' ', '').replace("'", '' )[1:-1]
    url = ALLTIME_SALES_URL.format(app_ids=app_id_list, end_date=date, auth_token=token)
    response = requests.get(url)
    if response:
        df = pd.read_json(response.text)
        if df.empty:
            return pd.DataFrame(columns=['add_id', 'units_cumulative', 'revenue_cumulative'])
        df = df[['app_id', 'unified_units', 'unified_revenue']]
        df = df.groupby('app_id').sum()
        print("Done.")
        df = df.rename(columns={'unified_units': 'units_cumulative', 'unified_revenue':'revenue_cumulative'})
        return df
    else:
        print(response)
        return pd.DataFrame(columns=['add_id', 'units_cumulative', 'revenue_cumulative'])


def get_app_names(token, app_ids):
    print('Querying app name, publisher name and release date...')
    df = pd.DataFrame(columns=['app_id', 'name', 'publisher_id', 'publisher_name'])
    for i in range(0, len(app_ids), 100):
        end = min(len(app_ids), i+100)
        app_id_list = str(app_ids[i:end]).replace(' ', '').replace("'", '' )[1:-1]
        url = APPS_URL.format(app_ids=app_id_list, auth_token=token)
        response = requests.get(url)
        if response:
            idf = pd.DataFrame(json.loads(response.text)['apps'])[['unified_app_id', 'name', 'unified_publisher_ids']]
            idf = idf.rename(columns={'unified_app_id': 'app_id', 'unified_publisher_ids': 'publisher_id'})
            idf['publisher_id'] = idf['publisher_id'].apply(lambda x: x[0])
            pdf = get_app_publishers(token, list(idf['publisher_id']))
            idf = pd.merge(idf, pdf, on='publisher_id')
            df = pd.concat([df, idf])
        else:
            print(response.text)
            return df
    print('Done.')
    return df

def get_android_release_dates(token, app_ids):
    print('Querying android app release dates...')
    df = pd.DataFrame(columns=['android_id', 'android_release_date'])
    for i in range(0, len(app_ids), 100):
        end = min(len(app_ids), i+100)
        app_id_list = str(app_ids[i:end]).replace(' ', '').replace("'", '' )[1:-1]
        response = requests.get(APP_ANDROID_INFO.format(app_ids=app_id_list, auth_token=token))
        if response:
            idf = pd.DataFrame(json.loads(response.text)['apps'])[['app_id', 'release_date']]
            idf['release_date'] = idf['release_date'].apply(lambda x: x[0: x.find('T')] if x else None)
            idf = idf.rename(columns={'app_id': 'android_id', 'release_date': 'android_release_date'})
            df = pd.concat([df, idf])
        else:
            print(response.text)
            return df
    print('Done.')
    return df

def get_ios_release_dates(token, app_ids):
    print('Querying ios app release dates...')
    df = pd.DataFrame(columns=['ios_id', 'ios_release_date'])
    for i in range(0, len(app_ids), 100):
        end = min(len(app_ids), i+100)
        app_id_list = str(app_ids[i:end]).replace(' ', '').replace("'", '' )[1:-1]
        response = requests.get(APP_IOS_INFO.format(app_ids=app_id_list, auth_token=token))
        if response:
            idf = pd.DataFrame(json.loads(response.text)['apps'])
            idf = idf[['app_id', 'release_date']]
            idf['release_date'] = idf['release_date'].apply(lambda x: x[0: x.find('T')] if x else None)
            idf = idf.rename(columns={'app_id': 'ios_id', 'release_date': 'ios_release_date'})
            df = pd.concat([df, idf])
        else:
            print(response.text)
            return df
    print('Done.')
    return df

def get_app_publishers(token, publisher_ids):
    df = pd.DataFrame(columns=['publisher_id', 'publisher_name'])
    for i in range(0, len(publisher_ids), 100):
        end = min(len(publisher_ids), i+100)
        publisher_id_list = str(publisher_ids[i:end]).replace(' ', '').replace("'", '' )[1:-1]
        url = PUBLISHER_URL.format(publisher_ids=publisher_id_list, auth_token=token)
        response = requests.get(url)
        if response:
            idf = pd.DataFrame(json.loads(response.text)['publishers'])
            idf = idf[['unified_publisher_id', 'unified_publisher_name']].rename(columns={'unified_publisher_id': 'publisher_id', 'unified_publisher_name':'publisher_name'})
            df = pd.concat([df, idf])
        else:
            print(response.text)
            return df
    return df


if __name__=="__main__":
    main()

print()