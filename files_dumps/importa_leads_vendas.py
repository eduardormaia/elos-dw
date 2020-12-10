import pandas as pd
from os import walk
import datetime
import psycopg2
import numpy as np
import datetime as date

arquivo = input("Digite o arquivo desejado: ")
arquivo = int(arquivo)

mypath = r"G:\Meu Drive\Nova Espaçolaser\05. BI\VD_ELOS\\base\\"


f = []
g = []
for (dirpath, dirnames, filenames) in walk(mypath):
    f.extend(filenames)

w = 0

for i in f:
    if ('.xls' in i and '~' not in i):
        g.append(mypath+i)



w = w + 1
print("Started File: " + str(g[arquivo]))        
df = pd.read_excel(g[arquivo])


df['Valor Contrato'] = df['Valor Contrato'].astype(str)
df['CPF Atendente'] = df['CPF Atendente'].astype(str)
df['Conversão Lead - Cliente (horas)'] = df['Conversão Lead - Cliente (horas)'].astype(str)
df['Telefone'] = df['Telefone'].str.replace(r'\D+','',regex=True)
df['Telefone'] = df['Telefone'].str[:11]
df['Telefone'] = df['Telefone'].replace(np.nan, 0)
df['Cadastro Lead'] = pd.to_datetime(df['Cadastro Lead'],dayfirst=True) + date.timedelta(hours = 3 )
df['Cadastro Cliente'] = pd.to_datetime(df['Cadastro Cliente'],dayfirst=True) + date.timedelta(hours = 3 )
df['Data Contrato'] = pd.to_datetime(df['Data Contrato'],dayfirst=True) + date.timedelta(hours = 3 )
df['CpfCnpj'] = df['CpfCnpj'].astype(str)

    # df['Contrato'] = df['Contrato'].values.astype(str)
    # df['Valor do Gift Card'] = df['Valor do Gift Card'].values.astype(str)
    # df['CPF Vendedor'] = df['CPF Vendedor'].values.astype(str)
    # df['V. Desconto'] = df['V. Desconto'].values.astype(float)
    # df['Valor da Taxa'] = df['Valor da Taxa'].values.astype(float)
    # df['Precisa de Aprov.'] = df['Precisa de Aprov.'].values.astype(float)
    # df['V. Bruto'] = df['V. Bruto'].values.astype(float)   
    # df['É Cortesia?'] = df['É Cortesia?'].values.astype(float)

con = psycopg2.connect(
    host = '201.48.32.145',#'10.115.87.26',
    user = 'pessoalize',
    password = 'pess123',
    dbname = 'elos_dw',
    port = 3387##5432
)
cur = con.cursor()
r,c = df.shape
for index,row in df.iterrows():
    cur.execute("""
    INSERT INTO public.elos_leads_vendas
(lead_nome, cliente_nome, cpfcnpj, email, telefone, midia, cadastro_lead, cadastro_cliente, estabelecimento, codigo_contrato, data_contrato, valor_contrato, nome_atendente, cpf_atendente, perfil_atendente)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """,(df['Lead Nome'][index],df['Cliente Nome'][index],df['CpfCnpj'][index],df['Email'][index],df['Telefone'][index],df['Midia'][index],df['Cadastro Lead'][index],df['Cadastro Cliente'][index],df['Estabelecimento'][index],df['Codigo Contrato'][index],df['Data Contrato'][index],df['Valor Contrato'][index],df['Nome Atendente'][index],df['CPF Atendente'][index],df['Perfil Atendente'][index]))

con.commit()
print("Finished File: " + str(g[arquivo]))        
