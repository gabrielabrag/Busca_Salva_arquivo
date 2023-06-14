
import pandas as pd
import glob
import os.path
import pyodbc
import numpy as np


#pega ultimo csv da pasta
folder_path = r'C:\caminho'
file_type = '\*csv' # se nao quiser filtrar por extenção deixe apenas *
files = glob.glob(folder_path + file_type)
max_file = max(files, key=os.path.getctime)

#atribuindo ao df
df = pd.read_csv((max_file))
df.columns = (df.columns.str.replace(' ', ''))
df=df.astype(str)
df = df.replace(['nan','None'], '', regex=True)
#print(df)

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

