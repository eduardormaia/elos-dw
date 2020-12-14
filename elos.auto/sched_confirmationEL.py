import requests
import pandas as pd
import json
import datetime
import numpy as np
import units
import hashlib
import psycopg2.extras
import time
import os
from os import sys



def get_sched(units,StartDate,EndDate):
    df = pd.DataFrame()
    units = units.unitis()
    unitid = units['Id'].tolist()
    Inicio = datetime.datetime.strftime(StartDate, "%d/%m/%Y")
    Fim = datetime.datetime.strftime(EndDate, "%d/%m/%Y")
    print(Inicio + ' a ' + Fim)
    for i in unitid:

        url = "https://espacolaser.evup.com.br/Report/SchedulerEvent/List"

        payload = "sort=&group=&filter=&dateStart=" + str(Inicio) + "&dateEnd=" + str(
            Fim) + "&Client=&Item=50000000&DetailLevel%5B0%5D=Date&DetailLevel%5B1%5D=Hour&DetailLevel%5B2%5D=Establishment&DetailLevel%5B3%5D=Locality&DetailLevel%5B4%5D=Status&DetailLevel%5B5%5D=Client&DetailLevel%5B6%5D=CreateUser&DetailLevel%5B7%5D=CreateUserProfile&DetailLevel%5B8%5D=Item&DetailLevel%5B9%5D=MediaType&DetailLevel%5B10%5D=SchedulerDate&DetailLevel%5B11%5D=Origin"
        headers = {
            'authority': 'espacolaser.evup.com.br',
            'accept': '*/*',
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://espacolaser.evup.com.br',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://espacolaser.evup.com.br/',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,gl;q=0.6,es;q=0.5',
            'cookie': "_ga=GA1.3.863268899.1599133710; _gid=GA1.3.225653813.1600086467; ASP.NET_SessionId=4disrs0byuthinwt2nm5yj4c; .ASPXAUTH=9FCFD8FBF2E60774FAEA11693769D4803148120981597785AAC561D0157BDB040A1A0445BC17DED98261BD1887C23BB42FEB1B82E58EE989C45AB92ABA7AF8D4E42954362FF919F43DF41E56F4182332402C4CCC; tz=America%2FSao_Paulo; slot-routing-url=-; current-organizational-structure=" + str(
                i)
        }
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            sched = json.loads(response.text.encode('utf8'))
            data = sched['Data']
            if len(data) != 0:
                df = df.append(data, ignore_index=True, sort=True)
  

        except:
            response = requests.request("POST", url, headers=headers, data=payload)
            print(response)
            print(response.text)

    df['ClientPhone'] = df['ClientPhone'].str.replace(r'\D+', '', regex=True)
    df['ClientPhone'] = df['ClientPhone'].replace(np.nan, 0)
    df['Date'] = df['Date'].str.extract(r'(\d+)')
    df['Date'] = pd.to_datetime(df['Date'], unit='ms')
    df['Date'] = df['Date'].replace({pd.NaT: None})
    df['Date'] = df['Date'].replace({np.nan: None})
    df['SchedulerDate'] = df['SchedulerDate'].str.extract(r'(\d+)')
    df['SchedulerDate'] = pd.to_datetime(df['SchedulerDate'], unit='ms')
    df['SchedulerDate'] = df['SchedulerDate'].replace({pd.NaT: None})
    df['Hour'] = df['Hour'].str.extract(r'(\d+)')
    df['Hour'] = pd.to_datetime(df['Hour'], unit='ms')
    df['Hour'] = df['Hour'].replace({pd.NaT: None})

    return df




def hash(sourcedf, destinationdf, *column):
    columnName = ''
    destinationdf['hashid'] = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))[0].str.encode('utf-8').apply(
        lambda x: (hashlib.md5(x).hexdigest()))



def insert(df, conn, table):
    try:
        print('Inserting dataframe into DB...')
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        aux = '(' + '%s,' * (len(tuples[0]) - 1) + '%s)'
        query = "INSERT INTO " + table + "(" + cols + ") VALUES " + aux
        cursor = conn.cursor()
        psycopg2.extras.execute_batch(cursor, query, tuples)
        conn.commit()
        cursor.close()
    except:
        print('Erro: ' + str(sys.exc_info()))
        df.to_csv(os.path.join(directory_of_python_script, "test.csv"))
    return 'Done'





def insert_elos_key(conn, table):
    print('Inserting missing keys_elos...')
    cursor = conn.cursor()
    cursor.execute("""UPDATE """+ table +""" es
set KEY_ELOS = concat(substring(es.client, 1, 8), cast(es.clientphone as text))
where sched_id in (
    select sched_id
    FROM """+ table + """ES
    where key_elos is null
    )""")
    conn.commit()
    cursor.close()



def delete_dup_hashid(conn,Startdate):
    linhas = 1
    while linhas != 0:
        print('Deleting duplicated rows...')
        qry = """DELETE FROM """ + table + """
        WHERE sched_id IN 
        (SELECT MIN(sched_id) FROM """+ table +"""
        WHERE DATE >= '""" + str(StartDate) + """'
        GROUP BY hashid
        HAVING COUNT(1) > 1)"""
        cursor = conn.cursor()
        cursor.execute(qry)
        linhas = cursor.rowcount
        print('Rows affected: ' + str(cursor.rowcount))
        conn.commit()



def delete_errors(conn,Startdate):
    linhas = 1
    while linhas != 0:
        print('Deleting duplicated rows...')
        qry = """DELETE FROM """ + table + """
        WHERE sched_id IN 
        (SELECT MIN(sched_id) FROM """+ table +"""
        WHERE DATE >= '""" + str(StartDate) + """'
        GROUP BY hashid
        HAVING COUNT(1) > 1)"""
        cursor = conn.cursor()
        cursor.execute(qry)
        linhas = cursor.rowcount
        print('Rows affected: ' + str(cursor.rowcount))
        conn.commit()


table = 'elos_sched_3 '

directory_of_python_script = os.path.dirname(os.path.abspath(__file__))



pd.set_option('display.max_columns', 20)


while True:

    now = datetime.datetime.now()
    since = now.date() - datetime.timedelta(days= 2)
    StartDate = now.date() - datetime.timedelta(days= 2)
    LimitDate = StartDate + datetime.timedelta(days=30)
    EndDate = StartDate + datetime.timedelta(days=15)
    

    while True:
        conn = psycopg2.connect(
        host='10.115.87.26',
        user='pessoalize',
        password='pess123',
        dbname='elos_dw',
        port=5432)

        df = get_sched(units,StartDate,EndDate)  # Generating dataframe

        columns = ['ClientPhone', 'CreateUser', 'Date', 'Item', 'SchedulerDate']
        df = df.replace(r'^\s*$', 0, regex=True)
        df['ClientPhone'] = df['ClientPhone'].astype(str).fillna('')
        df['CreateUser'] = df['CreateUser'].astype(str).fillna('')
        df['SchedulerDate'] = df['SchedulerDate'].astype(str)
        df['PhoneHash'] = df['ClientPhone'].str[-5:]

        hash(df, df, 'PhoneHash', 'CreateUser','Item', 'SchedulerDate')  # Generating hash
        df = df.drop(columns=['PhoneHash'])
        insert(df, conn, table)  # Inserting table into database
        insert_elos_key(conn, table)  # Inserting elos_key
        delete_dup_hashid(conn,StartDate)
        conn.close()

        StartDate = EndDate + datetime.timedelta(days=1)
        EndDate = EndDate + datetime.timedelta(days=15)


        if StartDate > LimitDate:
            time.sleep(360)
        
    time.sleep(300)
    print("Sleeping for 5 minutes")