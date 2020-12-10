import pandas as pd
from os import walk
import datetime
import psycopg2
import psycopg2.extras
import numpy as np
import hashlib
pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', 26)


mypath = r"G:\\Meu Drive\\Nova Espaçolaser\\05. BI\\NV_FAT_ELOS\\"

arquivo = int(input("Informe o número do arquivo: "))

f = []
g = []
for (dirpath, dirnames, filenames) in walk(mypath):
    f.extend(filenames)

w = 0

for i in f:
    if '.xls' in i:
        g.append(mypath+i)

print(len(g))
print("Open file: " + str(g[arquivo]) + " ||| Index: " + str(arquivo))
df = pd.read_excel(g[arquivo])

df['ID Orç.'] = df['ID Orç.'].values.astype(str)
df['Cód. Externo'] = df['Cód. Externo'].values.astype(str)
df['Contrato'] = df['Contrato'].values.astype(str)
df['Valor do Gift Card'] = df['Valor do Gift Card'].values.astype(str)
df['CPF Vendedor'] = df['CPF Vendedor'].values.astype(str)
df['V. Desconto'] = df['V. Desconto'].values.astype(float)
df['Valor da Taxa'] = df['Valor da Taxa'].values.astype(float)
df['Precisa de Aprov.'] = df['Precisa de Aprov.'].values.astype(float)
df['V. Bruto'] = df['V. Bruto'].values.astype(float)
df['É Cortesia?'] = df['É Cortesia?'].values.astype(float)


df.rename(columns={'ID Orç.': 'BudgetId', 'Cód. Externo': 'ExternalCode', 'Orçamento': 'Budget','Contrato': 'ContractNumber','Status': 'StatusDescription','Origem Mídia': 'MediaType','Mês': 'Month','Data': 'Date','Estabelecimento': 'Establishment','Cliente': 'Client','Item': 'Item','Estabelecimento de origem': 'OrgOrigin','E-commerce': 'IsEcommerceDescription','Valor do Gift Card': 'GiftCardAmount','Utilização de  Gift Card': 'UseGiftCard','Vendedor': 'Salesman','Perfil': 'SalesmanProfile','CPF Vendedor': 'SalesmanCpfCnpj','Precisa de Aprov.': 'NeedApproval','É Cortesia?': 'IsGift','Nome da Campanha': 'CampaignName','Código do Voucher': 'VoucherCode','Valor da Taxa': 'FineValue','V. Bruto': 'GrossValue','V. Líquido': 'NetValue', 'V. Desconto':'DiscountAmount', 'V. Desconto.1':'DiscountValue' }, inplace=True)

r, c = df.shape
df['hashid'] = ''
print('Generating hashids...')
for index, data in df.iterrows():
    key = str(df['BudgetId'][index]) + str(df['Budget'][index]) + str(df['Date'][index]) + str(
        df['Item'][index]) + str(df['SalesmanCpfCnpj'][index])
    key_encode = key.encode("utf-8")
    hash = hashlib.md5(key_encode)
    hexa = hash.hexdigest()
    df['hashid'][index] = hexa

df = df[['BudgetId', 'ExternalCode', 'Budget', 'ContractNumber', 'StatusDescription', 'MediaType', 'Month', 'Date', 'Establishment', 'Client', 'Item', 'OrgOrigin', 'IsEcommerceDescription', 'GiftCardAmount', 'UseGiftCard', 'Salesman', 'SalesmanProfile', 'SalesmanCpfCnpj', 'NeedApproval', 'IsGift', 'CampaignName', 'VoucherCode', 'FineValue', 'GrossValue', 'DiscountValue', 'NetValue', 'hashid', 'DiscountAmount']]

conn = psycopg2.connect(
    host='10.115.87.26',
    user='pessoalize',
    password='pess123',
    dbname='elos_dw',
    port=5432
)
table = 'elos_faturamento_1'

print('Inserting dataframe into DB...')
tuples = [tuple(x) for x in df.to_numpy()]
cols = ','.join(list(df.columns))
aux = '(' + '%s,' * (len(tuples[0]) - 1) + '%s)'
query = "INSERT INTO " + table + "(" + cols + ") VALUES " + aux
cursor = conn.cursor()
psycopg2.extras.execute_batch(cursor, query, tuples)
conn.commit()
cursor.close()
print('Done')
conn.close()