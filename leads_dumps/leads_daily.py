import datetime as date
import psycopg2
import pandas as pd
from os import sys
from datetime import timedelta
import requests
import numpy as np
import time
import json


def insert_lead(name,phone,status,source,reference,unitid):


    try: 
      
        name = str(name).strip()
        status = str(status).strip()
        source = str(source).strip()
        reference = str(reference).strip()
        
        url = "http://app.indique.digital/api/leads/leadsource"

        payload = """
        {
        "name": "%s",
        "phone": "%s",
        "status":"%s",
        "source":"%s",
        "reference":"%s",
        "unitId":"%s"
        }""" %(name,str(phone),'NOVO',source,reference,unitid)
        headers = {'content-type': 'application/json'}
        while True:
            try:
                response = requests.request("POST", url, headers=headers, data = payload)
                break
            except:
                print('Erro: ' + str(sys.exc_info()[0]))
                time.sleep(10)
                response = requests.request("POST", url, headers=headers, data = payload)

        data = json.loads(response.text.encode('utf8'))
    except:
        data = 'Erro: ' + str(sys.exc_info())

    
    
    return(data)



#Extract units from pessoalize database
def leads_call(page,pagesize,DateStrInicio,DateStrFim):

    response = 0
   
    dflead = pd.DataFrame(columns=['Id','SerializedOldValue','CpfCnpj','PhoneCountryCode','PhoneAreaCode','PhoneNumber','Name','Status','StatusDesc','Workplace','Email','Gender','ContactTypeId','ContactType_Name','AttendantId','AttendantName','AttendantProfileName','AttendantCpfCpnj','Media_Id','Media_Name','Media_ShowPartnerField','Partner_Id','Partner_Name','StructureId','Notes','CreationDate','SchedulingDate','ConversionDate','ConversionTimespan','CompletePhone','Classifiers','IsInactive','OwnerOrgStruct_Id','OwnerOrgStruct_Description','IsBlockToCall','Client_Id','Client_Name','Indicated_By','IndicatedByLead_Id','IndicatedByLead_Name','ClientAuthId','ExternalTrackingCode','IndicatedBy_Name','IndicatedBy_CpfCnpj'])
    while True:
        url = "https://espacolaser.evup.com.br/Lead/ListFilteredLeads"

        payload = "sort=&page="+ str(page) +"&pageSize="+str(pagesize)+"&group=&filter=&name=&areaCode=&phone=&initialDate="+str(DateStrInicio)+"&finalDate="+str(DateStrFim)+"&attendant=&media=&status="
        headers = {
        'authority': 'espacolaser.evup.com.br',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://espacolaser.evup.com.br',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://espacolaser.evup.com.br/',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,gl;q=0.6,es;q=0.5',
        'cookie': 'ASP.NET_SessionId=4t5g2uullfojwavbhvsiwage; tz=America%2FSao_Paulo; .ASPXAUTH=593CA3C930C6E251FDC77C6A4D91A7A115A1E24CE9E528A69160E47A1054CD43A9C9D09FE066ABE6C9E7DE76A579E2F06459D7CD91E7580B55EDF7CF8B6D03D9EC61F17B77D57A6C1B91689AD26503EE6C5AA39E; slot-routing-url=-; current-organizational-structure=1'
        }

        try:
            response = requests.request("POST", url, headers=headers, data = payload)
        except:
            print('Erro: ' + str(sys.exc_info()[0]))
            time.sleep(3)
            print('Conection Lost')
            response = requests.request("POST", url, headers=headers, data = payload)

        data = json.loads(response.text.encode('utf8'))
        d = data['Data']

        if len(d) > 0:
            dflead = dflead.append(d, ignore_index=True,sort=True)
            page = page + 1
        else:
            break
    return(dflead)

def lead_list(DateInit,DateFinit):

    #Create empty DataFrame

    dflead = pd.DataFrame(columns=['Id','SerializedOldValue','CpfCnpj','PhoneCountryCode','PhoneAreaCode','PhoneNumber','Name','Status','StatusDesc','Workplace','Email','Gender','ContactTypeId','ContactType_Name','AttendantId','AttendantName','AttendantProfileName','AttendantCpfCpnj','Media_Id','Media_Name','Media_ShowPartnerField','Partner_Id','Partner_Name','StructureId','Notes','CreationDate','SchedulingDate','ConversionDate','ConversionTimespan','CompletePhone','Classifiers','IsInactive','OwnerOrgStruct_Id','OwnerOrgStruct_Description','IsBlockToCall','Client_Id','Client_Name','Indicated_By','IndicatedByLead_Id','IndicatedByLead_Name','ClientAuthId','ExternalTrackingCode','IndicatedBy_Name','IndicatedBy_CpfCnpj'])

    # Tranform Date extraction format

    DateStrInicio = date.datetime.strftime(DateInit,"%d/%m/%Y")
    DateStrFim = date.datetime.strftime(DateFinit,"%d/%m/%Y")
    #Parameters for request

    page = 1
    pagesize = 1000

    data = leads_call(page,pagesize,DateStrInicio,DateStrFim)


    #Call elos

    dflead = dflead.append(data, ignore_index=True,sort=True)

    #Transform Data

    dflead['PhoneNumber'] = dflead['PhoneNumber'].str.replace(r'\D+','',regex=True)
    dflead['PhoneNumber'] = dflead['PhoneNumber'].replace(np.nan, 0)
        
    dflead['CompletePhone'] = dflead['CompletePhone'].str.replace(r'\D+','',regex=True)
    dflead['CompletePhone'] = dflead['CompletePhone'].replace(np.nan, 0)

    dflead['PhoneAreaCode'] = dflead['PhoneAreaCode'].str.replace(r'\D+','',regex=True)
    dflead['PhoneAreaCode'] = dflead['PhoneAreaCode'].replace('',0)    
    dflead['PhoneAreaCode'] = dflead['PhoneAreaCode'].astype('float')   
    dflead['PhoneAreaCode'] = dflead['PhoneAreaCode'].replace(np.nan, 0)
 

    dflead['CreationDate'] = dflead['CreationDate'].str.extract(r'(\d+)')
    dflead['CreationDate'] = pd.to_datetime(dflead['CreationDate'], unit='ms')
    
    dflead['SchedulingDate'] = dflead['SchedulingDate'].str.extract(r'(\d+)')
    dflead['SchedulingDate'] = pd.to_datetime(dflead['SchedulingDate'], unit='ms')
    dflead['SchedulingDate'] = dflead['SchedulingDate'].replace({pd.NaT: None})

    dflead['ConversionDate'] = dflead['ConversionDate'].str.extract(r'(\d+)')
    dflead['ConversionDate'] = pd.to_datetime(dflead['ConversionDate'], unit='ms')
    dflead['ConversionDate'] = dflead['ConversionDate'].replace({pd.NaT: None})


    dflead['Name'] = dflead['Name'].str.replace(r'[^a-zA-Z\s]+','',regex = True)
    dflead['IndicatedBy_Name'] = dflead['IndicatedBy_Name'].str.replace(r'[^a-zA-Z\s]+','',regex = True)

    return(dflead)
        



pd.options.mode.chained_assignment = None  # default='warn'
con = psycopg2.connect(
    host = 'pessoalize-0302.ceftjzilvb04.sa-east-1.rds.amazonaws.com',
    user = 'pessoalize',
    password = 'AWS#152436',    
    dbname = 'pessoalize',
    port = 5432
)

cur = con.cursor()

cur.execute("""
select trim(replace(u.description,'  ',' ')),u.id from unit u
""")
total2 = cur.fetchall()

#Create dataframe psunits

psunits = pd.DataFrame(total2,columns=['Description','unitId'])


# Creates iterators

total = 0
added = 0
charge = 0

#Define Dates

now = date.datetime.now()
DateFinit = date.date(now.year,now.month,now.day)
early = now + date.timedelta(days = -1)
DateInit = date.date(early.year,early.month,early.day)

#Call function to get elosleads

data = lead_list(DateInit,DateFinit)
row, col = data.shape
r = row

#Extract existing leads from elos_dw

con = psycopg2.connect(
    host = '10.115.87.26',
    user = 'pessoalize',
    password = 'pess123',
    dbname = 'elos_dw',
    port = 5432
)
cur = con.cursor()
cur.execute("""SELECT id FROM elos_leads
WHERE creationdate >= (CURRENT_DATE - interval '3 DAY');""")
con.commit()  
total = cur.fetchall()
telephones = []
for i in range(len(total)):
    telephones.append(total[i][0])


#Cross elos_dw with elos extraction

df_db = pd.DataFrame(telephones,columns=['id'])
df_total = pd.merge(data,df_db , how='left', left_on='Id', right_on='id')
df_total = df_total[df_total['id'].isnull()]


# Start Insertion

df_total = df_total[df_total['CompletePhone'].str.len() > 1]

for index,row in df_total.iterrows():
    try:
        cur.execute("""
    INSERT INTO public.elos_leads
    (id, serializedoldvalue, cpfcnpj, phonecountrycode, phoneareacode, phonenumber, "name", status, statusdesc, workplace, email, gender, contacttypeid, contacttype_name, attendantid, attendantname, attendantprofilename, attendantcpfcpnj, media_id, media_name, media_showpartnerfield, partner_id, partner_name, structureid, notes, creationdate, schedulingdate, conversiondate, conversiontimespan, completephone, classifiers, isinactive, ownerorgstruct_id, ownerorgstruct_description, isblocktocall, client_id, client_name, indicated_by, indicatedbylead_id, indicatedbylead_name, clientauthid, externaltrackingcode, indicatedby_name, indicatedby_cpfcnpj,datelog) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """,(df_total['Id'][index],df_total['SerializedOldValue'][index],df_total['CpfCnpj'][index],df_total['PhoneCountryCode'][index],df_total['PhoneAreaCode'][index],df_total['PhoneNumber'][index],df_total['Name'][index],df_total['Status'][index],df_total['StatusDesc'][index],df_total['Workplace'][index],df_total['Email'][index],df_total['Gender'][index],df_total['ContactTypeId'][index],df_total['ContactType_Name'][index],df_total['AttendantId'][index],df_total['AttendantName'][index],df_total['AttendantProfileName'][index],df_total['AttendantCpfCpnj'][index],df_total['Media_Id'][index],df_total['Media_Name'][index],df_total['Media_ShowPartnerField'][index],df_total['Partner_Id'][index],df_total['Partner_Name'][index],df_total['StructureId'][index],df_total['Notes'][index],df_total['CreationDate'][index],df_total['SchedulingDate'][index],df_total['ConversionDate'][index],df_total['ConversionTimespan'][index],df_total['CompletePhone'][index],df_total['Classifiers'][index],df_total['IsInactive'][index],df_total['OwnerOrgStruct_Id'][index],df_total['OwnerOrgStruct_Description'][index],df_total['IsBlockToCall'][index],df_total['Client_Id'][index],df_total['Client_Name'][index],df_total['Indicated_By'][index],df_total['IndicatedByLead_Id'][index],df_total['IndicatedByLead_Name'][index],df_total['ClientAuthId'][index],df_total['ExternalTrackingCode'][index],df_total['IndicatedBy_Name'][index],df_total['IndicatedBy_CpfCnpj'][index],date.datetime.now()))
        added = added + 1
    except:
        print('Erro: ' + str(sys.exc_info()))
        print(row)
        continue

#Start Insertion API Indique Digital

df_total = df_total[df_total['StatusDesc'] == 'Pendente']
unit_errors = []
for index,row in df_total.iterrows():
    try: 

        df_total['OwnerOrgStruct_Description'][index] = df_total['OwnerOrgStruct_Description'][index].replace('  ',' ').strip()
        unitdesc = df_total['OwnerOrgStruct_Description'][index]
        if df_total['OwnerOrgStruct_Id'][index] not in unit_errors:
            unitId = psunits[psunits['Description'] == unitdesc]['unitId'].values[0]
            
            if len(df_total['CompletePhone'][index]) == 13:
                df_total['CompletePhone'][index] = df_total['CompletePhone'][index][2:]


            if len(df_total['CompletePhone'][index]) == 10:
                df_total['CompletePhone'][index] = str(df_total['CompletePhone'][index][0:2]) + '9' + str(df_total['CompletePhone'][index][2:])

            retorno = insert_lead(df_total['Name'][index],df_total['CompletePhone'][index],'NOVO',df_total['Media_Name'][index],df_total['IndicatedBy_Name'][index],unitId)
            charge = charge + 1

    except:
        print('Erro: ' + str(sys.exc_info()))
        unit_errors.append(row['OwnerOrgStruct_Id'])
        


con.commit()            


print('Data: '+ str(date.datetime.now().day)+ '/' + str(date.datetime.now().month) + '/' + str(date.datetime.now().year) + ' ' + str(date.datetime.now().hour) + ':' + str(date.datetime.now().minute) + ' Total Leads: ' + str(r) + " Adicionados: " + str(added) + " Importados: "+str(charge))
con.close()
con.close()
con.close()