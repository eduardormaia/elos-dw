import requests
import pandas as pd
import json


def unitis():    
  url = "https://espacolaser.evup.com.br/OrganizationalStructure/ListUser"

  payload = "sort=&group=&filter="
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
    'cookie': '_ga=GA1.3.863268899.1599133710; _gid=GA1.3.225653813.1600086467; ASP.NET_SessionId=4disrs0byuthinwt2nm5yj4c; tz=America%2FSao_Paulo; slot-routing-url=-; current-organizational-structure=495; .ASPXAUTH=1FF2E9150FD8B3F3D299C5BA91C74512DE477B97F6EAF02DEB8C8C33BD9F443651CC62A107B534605EDD72725AA43D7CBC96FEE5D0B2A8317DC5D60ED0B33B13CD6782C00C50AA67A01379BC062BA42C96266E96'
  }

  response = requests.request("POST", url, headers=headers, data = payload)

  data = json.loads(response.text.encode('utf8'))
  data = data['Data']
  df = pd.DataFrame(data)

  economic_group = df[df['Father_Id'] == 1]
  
  units = df[df['Father_Id'] != 1]
  units = units[units['Id'] != 1]
  units = units[units['IsRoot'] != 'false']
  economic_group = economic_group[['Id','Alias','Description','Inactive','IsRoot','IsLeaf','IsDomain','Father_Id','IsEnabled']]
  units = units[['Id','Alias','Description','Inactive','IsRoot','IsLeaf','IsDomain','Father_Id','IsEnabled']]

  return(units)



if __name__ == "__main__":
    print(unitis())