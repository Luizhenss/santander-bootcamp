import pandas as pd
import csv

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

    for key, conta in formatted_dict.items():
        if conta['CurrentAccount'] == int(cc):
            data.to_csv('ETL-python/CLIENTES.csv', mode='w',index=False, sep=';')
            return conta
        
    return False
    
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
            data.to_csv('ETL-python/CONTAS.csv', mode='w',index=False, sep=';')
            return contas['Account']
            

def load_balance(user_id):
    data = pd.read_csv('ETL-python/SALDO.csv', delimiter=';')

    # Inicializar o dicionário final
    contas = {}

    # Iterar pelas linhas do DataFrame
    for index, row in data.iterrows():
        # Formatar o número como um dígito de dois caracteres
        formatted_number = f"{row['Balance']:02}"
        
        # Criar um dicionário para a linha atual
        entry = {
            'IdClient': row['IdClient'],
            'Balance': row['Balance']
        }
        
        # Adicionar o dicionário à chave formatada no dicionário final
        contas[formatted_number] = entry

    for key, conta in contas.items():
        if conta['IdClient'] == int(user_id):
            data.to_csv('ETL-python/SALDO.csv', mode='w',index=False, sep=';')
            return conta

def bar():
    print(f'''
--------------------------------------------------------------------------------------------------''')
    
def start_program():
    
    cc = input(f'''
/----------------------------------------------------------------------\ 
|                                                                      |
|                            Bem-vindo!                                |
|                                                                      |
|        Para continuar, digite o número de sua conta corrente!        |
|                                                                      | 
\----------------------------------------------------------------------/
               
| ''' )
    
    restart = True
    while restart:
        bar()

        conta = load_client(cc)
        if conta == False:
            print('Conta não existente, digite uma conta válida.')
            bar()

            start_program()

        num_conta = load_account(conta['IdClient'])

        conta_saldo = load_balance(conta['IdClient'])

        print(f'''
Olá {conta['NameClient']}''') 

        restart = exec_actions(conta, num_conta, conta_saldo)

def exec_actions(conta, num_conta, conta_saldo):    

    action = input(f'''         
Qual ação deseja realizar?
/--------------------------------\ 
|                                |
|   ( 1 ) Consultar Conta        |
|   ( 2 ) Consultar Saldo        |
|   ( 3 ) Realizar movimentação  |
|                                |
\--------------------------------/
            
| ''')

    bar()

    if action == '1':
        print(f'''
Nome: {conta['NameClient']}
Conta Corrente: {str(conta['CurrentAccount']).zfill(3)}
Número da conta: {num_conta} ''')

        bar()
    elif action == '2':
        print(f'''
O seu saldo é de R${conta_saldo['Balance']:,.2f}
    '''
        )

        bar()

    elif action == '3':

        movimentacao = input(f'''
            
Qual movimentação deseja realizar?
                        
/------------------\ 
|                  |               
|  ( 1 ) Saque     |
|  ( 2 ) Depósito  |
|                  |             
\------------------/  

| ''')
        
        bar()
        
        if movimentacao == '1':

            val_saque = input(f'''
Qual valor deseja sacar?
| ''')  
            
            bar()
            if conta_saldo['Balance'] >= float(val_saque):
                novo_valor = conta_saldo['Balance'] - float(val_saque)

                arquivo_csv = 'ETL-python/SALDO.csv'
                linhas = []

                with open(arquivo_csv, 'r', newline='') as arquivo_csv:
                    leitor = csv.reader(arquivo_csv, delimiter=';')
                    for linha in leitor:
                        linhas.append(linha) 

                # Encontrar o índice da coluna 'IdClient' e 'Balance'
                cabecalho = linhas[0]
                indice_idclient = cabecalho.index('IdClient')
                indice_balance = cabecalho.index('Balance')

                # Encontrar a linha com IdClient igual a 1 e atualizar o Balance
                id_alvo = str(conta['IdClient'])

                for linha in linhas:
                    if linha[indice_idclient] == id_alvo:
                        linha[indice_balance] = str(f'{novo_valor:,.2f}')
                        break 

                # Escrever as linhas de volta no CSV
                arquivo_csv = 'ETL-python/SALDO.csv'
                with open(arquivo_csv, 'w', newline='') as arquivo_csv:
                    escritor = csv.writer(arquivo_csv, delimiter=';')
                    escritor.writerows(linhas)

                print(f'Valor sacado com sucesso em sua conta, seu novo saldo é de: R${novo_valor:,.2f}')
                bar()
            else:
                print('Conta não possui valor dísponivel para saque!')
                bar()
        elif movimentacao == '2':
            val_saque = input(f'''
Qual valor deseja depositar?
| ''')

            bar()

            novo_valor = conta_saldo['Balance'] + float(val_saque)

            arquivo_csv = 'ETL-python/SALDO.csv'
            linhas = []

            with open(arquivo_csv, 'r', newline='') as arquivo_csv:
                leitor = csv.reader(arquivo_csv, delimiter=';')
                for linha in leitor:
                    linhas.append(linha) 

            # Encontrar o índice da coluna 'IdClient' e 'Balance'
            cabecalho = linhas[0]
            indice_idclient = cabecalho.index('IdClient')
            indice_balance = cabecalho.index('Balance')

            # Encontrar a linha com IdClient igual a 1 e atualizar o Balance
            id_alvo = str(conta['IdClient'])

            for linha in linhas:
                if linha[indice_idclient] == id_alvo:
                    linha[indice_balance] = str(f'{novo_valor:,.2f}')
                    break 

            # Escrever as linhas de volta no CSV
            arquivo_csv = 'ETL-python/SALDO.csv'
            with open(arquivo_csv, 'w', newline='') as arquivo_csv:
                escritor = csv.writer(arquivo_csv, delimiter=';')
                escritor.writerows(linhas)

            print(f'Valor depositado com sucesso em sua conta, seu novo saldo é de: R${novo_valor:,.2f}')
            bar()
            
    restart = input(f'''
Deseja realizar outra operação?
    
/-----------------\ 
|                 |               
|    ( 1 ) Sim    |
|    ( 2 ) Não    |
|                 |             
\-----------------/  
    
| ''')

    bar()

    return True if restart == '1' else False 

start_program()

'''
    Realizar ETL(Extract, Transform, Load), 
    para consultar contas, saldos, realizar movimentações e contas inativas ou ativas de clientes de um banco

    1 - Consultar Conta
    2 - Consultar Saldo
    3 - Realizar movimentação

'''
