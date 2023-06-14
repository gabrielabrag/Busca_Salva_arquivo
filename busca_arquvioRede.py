from smb.SMBConnection import SMBConnection
import datetime
import pandas as pd
from datetime import date
import pyodbc

SMB_SERVER_CLIENT='CLIENT'
SMB_SERVER_DOMAIN='DOMAIN'
SMB_SERVER_IP='IP'
SMB_SERVER_NAME='name'
SMB_SERVER_PASSWORD='PASSWORD'
SMB_SERVER_PORT= 'PORT'
SMB_SERVER_USER='USER'
SMB_SERVER_PATH='PATH\pastaRede'


#conectando com rede
conn = SMBConnection(SMB_SERVER_USER, SMB_SERVER_PASSWORD,SMB_SERVER_CLIENT, SMB_SERVER_NAME,use_ntlm_v2=True, domain=SMB_SERVER_DOMAIN)
conn.connect(SMB_SERVER_IP, SMB_SERVER_PORT)

#listar arquivos da pasta
file_list = conn.listPath('pastaRede', 'pasta')

# Criar uma lista de tuplas contendo o nome do arquivo e a data de criação 
files_with_creation_time = [(file.filename, datetime.datetime.fromtimestamp(file.create_time)) for file in file_list] 

# Ordenar a lista de arquivos pela data de criação 
files_with_creation_time.sort(key=lambda x: x[1])
file_name = files_with_creation_time[-1][0]
caminhoorigem= 'pasta/'+(file_name)

#criar pasta local para receber documento
caminho_local='./nomearquivo.csv'
# Abre o arquivo local para gravação
with open(caminho_local, 'wb') as file:
    # Baixa o arquivo do servidor SMB
    j = conn.retrieveFile('pastaRede', (caminhoorigem), file)
    #return True

#lendo e editando df 
df = pd.read_csv('nomearquivo.csv')
df.columns = (df.columns.str.replace(' ', ''))
df=df.astype(str)
df = df.replace(['nan','None'], '', regex=True)
print(df)

#salva no banco 
server = 'ip_servidor'
database = 'banco_dados' 
username = 'usuario' 
password = 'senha' 
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
    cursor.execute("INSERT INTO tabela (coluna_tabela,coluna_tabela,coluna_tabela) values(?,?,?)",row.colunadf,row.colunadf,row.colunadf)
cnxn.commit()
cursor.close()




