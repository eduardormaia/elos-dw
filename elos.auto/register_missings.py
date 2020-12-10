import datetime as date
import psycopg2
import pandas as pd
from os import sys
from datetime import timedelta
import requests
import numpy as np
a = 0


df_upd = pd.DataFrame()
pd.options.mode.chained_assignment = None  # default='warn'

con = psycopg2.connect(
    host = '10.115.87.26',
    user = 'pessoalize',
    password = 'pess123',
    dbname = 'elos_dw',
    port = 5432
)

cur = con.cursor()
linhas = 1
while linhas != 0:
    cur.execute("""select
        hour,
        key_elos,
        sched_id,
        statusdescription
    from
        elos_sched_JV es
    where
        es.key_elos in (select key_elos from elos_sched_JV es where qtd_faltas is null and item = 'AVALIAÇÃO' limit 1)
        and item = 'AVALIAÇÃO'
        and qtd_faltas is  null
    order by
        hour desc;
    """)
    con.commit()  
    total = cur.fetchall()
    linhas = cur.rowcount

    print(linhas)
    df = pd.DataFrame(total, columns=["Data","Chave","Sched_id","Status"])
    df['Data'] = pd.to_datetime(df['Data'],"%Y-%m-%d, %H:%M:%S")
    df['qtd_faltas'] = np.nan

    for index, row in df.iterrows():
        df2 = df[(df['Data'] < df['Data'][index]) & (df['Status'] != "Finalizado")]
        
        r = df2.shape[0]
        df['qtd_faltas'][index] = r
        
        cur.execute("""update elos_sched_jv set qtd_faltas = """+ str(df['qtd_faltas'][index]) +""" where sched_id = """ + str(df['Sched_id'][index]))
        a = a + 1
    con.commit()

    print("Ended: " + str(a))