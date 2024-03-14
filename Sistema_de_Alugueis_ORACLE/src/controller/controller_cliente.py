from model.clientes import Cliente
from conexion.oracle_queries import OracleQueries

class Controller_Cliente:
    def __init__(self):
        pass
        
    def inserir_cliente(self) -> Cliente:
        aux_loop = "s"
        while aux_loop == "s":
            ''' Ref.: https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#anonymous-pl-sql-blocks'''
            
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()
    
            # Solicita ao usuario o novo CPF
            cpf = input("CPF (Novo): ")
    
            if self.verifica_existencia_cliente(oracle, cpf):
                # Solicita ao usuario o novo nome
                nome = input("Nome (Novo): ")
                # Insere e persiste o novo cliente
                oracle.write(f"insert into clientes values ('{cpf}', '{nome}')")
                # Recupera os dados do novo cliente criado transformando em um DataFrame
                df_cliente = oracle.sqlToDataFrame(f"select cpf, nome from clientes where cpf = '{cpf}'")
                # Cria um novo objeto Cliente
                novo_cliente = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
                # Exibe os atributos do novo cliente
                print(novo_cliente.to_string())
                aux_loop = input("Deseja inserir mais um cliente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O CPF {cpf} já está cadastrado.")
                aux_loop = input("Deseja tentar inserir um cliente novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
                

    def atualizar_cliente(self) -> Cliente:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do cliente a ser alterado
            cpf = int(input("CPF do cliente que deseja alterar o nome: "))

            # Verifica se o cliente existe na base de dados
            if not self.verifica_existencia_cliente(oracle, cpf):
                # Solicita a nova descrição do cliente
                novo_nome = input("Nome (Novo): ")
                # Atualiza o nome do cliente existente
                oracle.write(f"update clientes set nome = '{novo_nome}' where cpf = {cpf}")
                # Recupera os dados do novo cliente criado transformando em um DataFrame
                df_cliente = oracle.sqlToDataFrame(f"select cpf, nome from clientes where cpf = {cpf}")
                # Cria um novo objeto cliente
                cliente_atualizado = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
                # Exibe os atributos do novo cliente
                print(cliente_atualizado.to_string())
                aux_loop = input("Deseja atualizar mais um cliente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O CPF {cpf} não existe.")
                aux_loop = input("Deseja tentar atualizar um cliente novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_cliente(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()
    
            # Solicita ao usuário o CPF do Cliente a ser alterado
            cpf = int(input("CPF do Cliente que irá excluir: "))        
    
            # Verifica se o cliente existe na base de dados
            if not self.verifica_existencia_cliente(oracle, cpf):            
                # Recupera os dados do novo cliente criado transformando em um DataFrame
                df_cliente = oracle.sqlToDataFrame(f"select cpf, nome from clientes where cpf = {cpf}")
                # Verifica se existem alugueis em nome do cliente
                if not self.verifica_existencia_cliente_em_alugueis(oracle, cpf):
                    print("O cliente possui aluguel. Não é possível excluir!")
                    aux_loop = input("Deseja tentar excluir outro cliente? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
                else:
                    opcao_excluir = input(f"Tem certeza que deseja excluir o cliente de CPF {cpf} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Remove o cliente da tabela
                        oracle.write(f"delete from clientes where cpf = {cpf}")            
                        # Cria um novo objeto Cliente para informar que foi removido
                        cliente_excluido = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
                        # Exibe os atributos do cliente excluído
                        print("Cliente Removido com Sucesso!")
                        print(cliente_excluido.to_string())
                        aux_loop = input("Deseja excluir mais um cliente? (S ou N)\n").lower()
                        aux_Skip = aux_loop
                        if aux_loop == "n":
                            break;
                    if aux_Skip != "s":
                        aux_loop = input("Ainda deseja excluir um cliente? (S ou N)\n").lower()
                        if aux_loop == "n":
                            break;
                    
            else:
                print(f"O CPF {cpf} não existe.")
                aux_loop = input("Deseja tentar excluir outro cliente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_cliente(self, oracle:OracleQueries, cpf:str=None) -> bool:
        # Recupera os dados do novo cliente criado transformando em um DataFrame
        df_cliente = oracle.sqlToDataFrame(f"select cpf, nome from clientes where cpf = {cpf}")
        return df_cliente.empty

    def verifica_existencia_cliente_em_alugueis(self, oracle:OracleQueries, cpf:str=None) -> bool:
        # Recupera os dados do novo cliente criado transformando em um DataFrame
        df_cliente = oracle.sqlToDataFrame(f"select cpf from alugueis where cpf = {cpf}")
        return df_cliente.empty
