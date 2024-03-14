from pydoc import cli
from model.alugueis import Aluguel
from model.clientes import Cliente
from controller.controller_cliente import Controller_Cliente
from model.montadoras import Montadora
from controller.controller_montadora import Controller_Montadora
from conexion.oracle_queries import OracleQueries
from datetime import date
import datetime

class Controller_Aluguel:
    def __init__(self):
        self.ctrl_cliente = Controller_Cliente()
        self.ctrl_montadora = Controller_Montadora()
        
    def inserir_aluguel(self) -> Aluguel:
        aux_loop = "s"
        while aux_loop == "s":
            ''' Ref.: https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#anonymous-pl-sql-blocks'''

            # Cria uma nova conexão com o banco
            oracle = OracleQueries()

            # Lista os clientes existentes para inserir no aluguel
            self.listar_clientes(oracle, need_connect=True)
            cpf = str(input("Digite o número do CPF do Cliente: "))
            cliente = self.valida_cliente(oracle, cpf)
            if cliente == None:
                return None

            # Lista os montadoras existentes para inserir no aluguel
            self.listar_montadoras(oracle, need_connect=True)
            cnpj = str(input("Digite o número do CNPJ da Montadora: "))
            montadora = self.valida_montadora(oracle, cnpj)
            if montadora == None:
                return None

            print("\nA data de data_aluguel_inicial será a data de hoje!\n")
            data_hoje = date.today()
            quantDiasSomar = int(input("Insira numero de dias a ser adicionado a data de hoje para data_aluguel_final: "))
            data_amanha = date.today() + datetime.timedelta(quantDiasSomar)

            # Recupera o cursos para executar um bloco PL/SQL anônimo
            cursor = oracle.connect()
            # Cria a variável de saída com o tipo especificado
            output_value = cursor.var(int)

            # Cria um dicionário para mapear as variáveis de entrada e saída
            data = dict(codigo=output_value, data_aluguel_inicial=data_hoje, data_aluguel_final=data_amanha, cpf=cliente.get_CPF(), cnpj=montadora.get_CNPJ())
            # Executa o bloco PL/SQL anônimo para inserção do novo veiculo e recuperação da chave primária criada pela sequence
            cursor.execute("""
            begin
                :codigo := ALUGUEIS_CODIGO_ALUGUEL_SEQ.NEXTVAL;
                insert into alugueis values(:codigo, :data_aluguel_inicial, :data_aluguel_final, :cpf, :cnpj);
            end;
            """, data)
            # Recupera o código do novo veiculo
            codigo_aluguel = output_value.getvalue()
            # Persiste (confirma) as alterações
            oracle.conn.commit()
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_aluguel = oracle.sqlToDataFrame(f"select codigo_aluguel, data_aluguel_inicial, data_aluguel_final from alugueis where codigo_aluguel = {codigo_aluguel}")
            # Cria um novo objeto Veiculo
            novo_aluguel = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0],  cliente, montadora)
            # Exibe os atributos do novo veiculo
            print(novo_aluguel.to_string())
            aux_loop = input("Deseja inserir mais um aluguel? (S ou N)\n").lower()
            if aux_loop == "n":
                break;

    def atualizar_aluguel(self) -> Aluguel:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_aluguel = int(input("Código do Aluguel que irá alterar: "))        

            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_aluguel(oracle, codigo_aluguel):

                # Lista os clientes existentes para inserir no aluguel
                self.listar_clientes(oracle)
                cpf = str(input("Digite o número do CPF do Cliente: "))
                cliente = self.valida_cliente(oracle, cpf)
                if cliente == None:
                    return None

                # Lista os montadoras existentes para inserir no aluguel
                self.listar_montadoras(oracle)
                cnpj = str(input("Digite o número do CNPJ da Montadora: "))
                montadora = self.valida_montadora(oracle, cnpj)
                if montadora == None:
                    return None

                quantDiasSubtrair = int(input("Insira numero de dias a ser subtraido da data de hoje para data_aluguel_inicial: "))
                data_hoje = date.today() - datetime.timedelta(quantDiasSubtrair)
                quantDiasSomar = int(input("Insira numero de dias a ser adicionado a data de hoje para data_aluguel_final: "))
                data_amanha = date.today() + datetime.timedelta(quantDiasSomar)

                # Atualiza a descrição do veiculo existente
                oracle.write(f"update alugueis set cpf = '{cliente.get_CPF()}', cnpj = '{montadora.get_CNPJ()}', data_aluguel_inicial = to_date('{data_hoje}','yyyy-mm-dd'), data_aluguel_final = to_date('{data_amanha}','yyyy-mm-dd') where codigo_aluguel = {codigo_aluguel}")
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_aluguel = oracle.sqlToDataFrame(f"select codigo_aluguel, data_aluguel_inicial, data_aluguel_final from alugueis where codigo_aluguel = {codigo_aluguel}")
                # Cria um novo objeto Veiculo
                aluguel_atualizado = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
                # Exibe os atributos do novo veiculo
                print(aluguel_atualizado.to_string())
                aux_loop = input("Deseja atualizar mais um aluguel? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O código {codigo_aluguel} não existe.")
                aux_loop = input("Deseja tentar atualizar um aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_aluguel(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_aluguel = int(input("Código do Aluguel que irá excluir: "))        

            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_aluguel(oracle, codigo_aluguel):            
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_aluguel = oracle.sqlToDataFrame(f"select codigo_aluguel, data_aluguel_inicial, data_aluguel_final, cpf, cnpj from alugueis where codigo_aluguel = {codigo_aluguel}")
                cliente = self.valida_cliente(oracle, df_aluguel.cpf.values[0])
                montadora = self.valida_montadora(oracle, df_aluguel.cnpj.values[0])

                opcao_excluir = input(f"Tem certeza que deseja excluir o aluguel {codigo_aluguel} [S ou N]: ")
                if opcao_excluir.lower() == "s":
                    print("Atenção, caso o aluguel possua itens de aluguel, eles também serão excluídos!")
                    opcao_excluir = input(f"Tem certeza que deseja excluir o aluguel {codigo_aluguel} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Remove o veiculo da tabela
                        oracle.write(f"delete from itens_aluguel where codigo_aluguel = {codigo_aluguel}")
                        print("Itens do aluguel removidos com sucesso!")
                        oracle.write(f"delete from alugueis where codigo_aluguel = {codigo_aluguel}")
                        # Cria um novo objeto Veiculo para informar que foi removido
                        aluguel_excluido = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
                        # Exibe os atributos do veiculo excluído
                        print("Aluguel Removido com sucesso!")
                        print(aluguel_excluido.to_string())
                        aux_loop = input("Deseja excluir mais um aluguel? (S ou N)\n").lower()
                        aux_Skip = aux_loop
                        if aux_loop == "n":
                            break;
                if aux_Skip != "s":
                    aux_loop = input("Ainda deseja excluir um aluguel? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
            else:
                print(f"O código {codigo_aluguel} não existe.")
                aux_loop = input("Deseja tentar excluir um aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
                

    def verifica_existencia_aluguel(self, oracle:OracleQueries, codigo:int=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = oracle.sqlToDataFrame(f"select codigo_aluguel, data_aluguel_inicial, data_aluguel_final from alugueis where codigo_aluguel = {codigo}")
        return df_aluguel.empty

    def listar_clientes(self, oracle:OracleQueries, need_connect:bool=False):
        query = """
                select c.cpf
                    , c.nome 
                from clientes c
                order by c.nome
                """
        if need_connect:
            oracle.connect()
        print(oracle.sqlToDataFrame(query))

    def listar_montadoras(self, oracle:OracleQueries, need_connect:bool=False):
        query = """
                select m.cnpj
                    , m.razao_social
                    , m.nome_fantasia
                from montadoras m
                order by m.nome_fantasia
                """
        if need_connect:
            oracle.connect()
        print(oracle.sqlToDataFrame(query))

    def valida_cliente(self, oracle:OracleQueries, cpf:str=None) -> Cliente:
        if self.ctrl_cliente.verifica_existencia_cliente(oracle, cpf):
            print(f"O CPF {cpf} informado não existe na base.")
            return None
        else:
            oracle.connect()
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_cliente = oracle.sqlToDataFrame(f"select cpf, nome from clientes where cpf = {cpf}")
            # Cria um novo objeto cliente
            cliente = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
            return cliente

    def valida_montadora(self, oracle:OracleQueries, cnpj:str=None) -> Montadora:
        if self.ctrl_montadora.verifica_existencia_montadora(oracle, cnpj):
            print(f"O CNPJ {cnpj} informado não existe na base.")
            return None
        else:
            oracle.connect()
            # Recupera os dados da nova montadora criado transformando em um DataFrame
            df_montadora = oracle.sqlToDataFrame(f"select cnpj, razao_social, nome_fantasia from montadoras where cnpj = {cnpj}")
            # Cria um novo objeto montadora
            montadora = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
            return montadora