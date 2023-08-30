import pandas as pd

def load_client(cc):
    data = pd.read_csv('ETL-python/CLIENTES.csv', delimiter=';')

    # Inicializar o dicionário final
    formatted_dict = {}

    # Iterar pelas linhas do DataFrame
    for index, row in data.iterrows():
        # Formatar o número como um dígito de dois caracteres
        formatted_number = f"{row['IdClient']:02}"
        
        # Criar um dicionário para a linha atual
        entry = {
            'IdClient': row['IdClient'],
            'NameClient': row['NameClient'],
            'CurrentAccount': row['CurrentAccount']
        }
        
        # Adicionar o dicionário à chave formatada no dicionário final
        formatted_dict[formatted_number] = entry

    for key, contas in formatted_dict.items():
        if contas['CurrentAccount'] == int(cc):
            conta = contas
            print(conta)
            break

    return conta

def load_account(user_id):
    data = pd.read_csv('ETL-python/CONTAS.csv', delimiter=';')

    # Inicializar o dicionário final
    formatted_dict = {}

    # Iterar pelas linhas do DataFrame
    for index, row in data.iterrows():
        # Formatar o número como um dígito de dois caracteres
        formatted_number = f"{row['IdClient']:02}"
        
        # Criar um dicionário para a linha atual
        entry = {
            'IdClient': row['IdClient'],
            'Account': row['Account']
        }
        
        # Adicionar o dicionário à chave formatada no dicionário final
        formatted_dict[formatted_number] = entry

    for key, contas in formatted_dict.items():
        if contas['IdClient'] == int(user_id):
            conta = contas
            print(conta)
            break

    return conta['Account']

def load_balance(user_id):
    data = pd.read_csv('ETL-python/SALDO.csv', delimiter=';')

    # Inicializar o dicionário final
    formatted_dict = {}

    # Iterar pelas linhas do DataFrame
    for index, row in data.iterrows():
        # Formatar o número como um dígito de dois caracteres
        formatted_number = f"{row['IdClient']:02}"
        
        # Criar um dicionário para a linha atual
        entry = {
            'IdClient': row['IdClient'],
            'Balance': row['Balance']
        }
        
        # Adicionar o dicionário à chave formatada no dicionário final
        formatted_dict[formatted_number] = entry

    for key, contas in formatted_dict.items():
        if contas['IdClient'] == int(user_id):
            conta = contas
            print(conta)
            break

    return conta['Balance']

cc = input(f'''Bem-vindo! Para continuar, digite o número de sua conta corrente!
|''' )

conta = load_client(cc)

num_conta = load_account(conta['IdClient'])

saldo = load_balance(conta['IdClient'])
action = input(f'''
Olá {conta['NameClient']}          

Qual ação deseja realizar?
|-------------------------------|
|  ( 1 ) Consultar Conta        |
|  ( 2 ) Consultar Saldo        |
|  ( 3 ) Realizar movimentação  |
|-------------------------------|
               
|''')


'''
    Realizar ETL(Extract, Transform, Load), 
    para consultar contas, saldos, realizar movimentações e contas inativas ou ativas de clientes de um banco

    1 - Consultar Contas
    2 - Consultar Saldos
    3 - Realizar movimentação
    4 - Ver status da conta
'''