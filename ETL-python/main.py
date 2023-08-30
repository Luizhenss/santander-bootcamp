import pandas as pd

action = input('Qual ação deseja realizar?')


df = pd.read_csv('ETL-python/CONTAS.csv', delimiter=';')
user_accounts = df['IdClient'].tolist()
print(user_accounts)

'''
    Realizar ETL(Extract, Transform, Load), 
    para consultar contas, saldos e contas inativas ou ativas de clientes de um banco
'''