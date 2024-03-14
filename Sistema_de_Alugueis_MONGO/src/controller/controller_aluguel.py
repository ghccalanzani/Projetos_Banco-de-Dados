import pandas as pd
from bson import ObjectId

from reports.relatorios import Relatorio

from model.alugueis import Aluguel
from model.clientes import Cliente
from model.montadoras import Montadora

from controller.controller_cliente import Controller_Cliente
from controller.controller_montadora import Controller_Montadora

from conexion.mongo_queries import MongoQueries
from datetime import datetime
from datetime import date
import datetime

class Controller_Aluguel:
    def __init__(self):
        self.ctrl_cliente = Controller_Cliente()
        self.ctrl_montadora = Controller_Montadora()
        self.mongo = MongoQueries()
        self.relatorio = Relatorio()
        
    def inserir_aluguel(self) -> Aluguel:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco
            self.mongo.connect()

            # Lista os clientes existentes para inserir no aluguel
            self.relatorio.get_relatorio_clientes()
            cpf = str(input("Digite o número do CPF do Cliente: "))
            cliente = self.valida_cliente(cpf)
            if cliente == None:
                return None

            # Lista as montadoras existentes para inserir no aluguel
            self.relatorio.get_relatorio_montadoras()
            cnpj = str(input("Digite o número do CNPJ da Montadora: "))
            montadora = self.valida_montadora(cnpj)
            if montadora == None:
                return None

            print("\nA data de data_aluguel_inicial será a data de hoje!\n")
            data_hoje = date.today().strftime("%m-%d-%Y")
            quantDiasSomar = int(input("Insira numero de dias a ser adicionado a data de hoje para data_aluguel_final: "))
            data_amanha = (date.today() + datetime.timedelta(quantDiasSomar)).strftime("%m-%d-%Y")

            proximo_aluguel = self.mongo.db["alugueis"].aggregate([
                                                                {
                                                                    '$group': {
                                                                        '_id': '$alugueis', 
                                                                        'proximo_aluguel': {
                                                                            '$max': '$codigo_aluguel'
                                                                        }
                                                                    }
                                                                }, {
                                                                    '$project': {
                                                                        'proximo_aluguel': {
                                                                            '$sum': [
                                                                                '$proximo_aluguel', 1
                                                                            ]
                                                                        }, 
                                                                        '_id': 0
                                                                    }
                                                                }
                                                            ])

            proximo_aluguel = int(list(proximo_aluguel)[0]['proximo_aluguel'])
            # Cria um dicionário para mapear as variáveis de entrada e saída
            data = dict(codigo_aluguel=proximo_aluguel, data_aluguel_inicial=data_hoje, data_aluguel_final=data_amanha, cpf=cliente.get_CPF(), cnpj=montadora.get_CNPJ())
            # Insere e Recupera o código do novo aluguel
            id_aluguel = self.mongo.db["alugueis"].insert_one(data)
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_aluguel = self.recupera_aluguel(id_aluguel.inserted_id)
            # Cria um novo objeto Veiculo
            novo_aluguel = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
            # Exibe os atributos do novo veiculo
            print(novo_aluguel.to_string())
            self.mongo.close()
            aux_loop = input("Deseja inserir mais um aluguel? (S ou N)\n").lower()
            if aux_loop == "n":
                break;

    def atualizar_aluguel(self) -> Aluguel:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()
    
            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_aluguel = int(input("Código do Aluguel que irá alterar: "))        
    
            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_aluguel(codigo_aluguel):
            
                # Lista os clientes existentes para inserir no aluguel
                self.relatorio.get_relatorio_clientes()
                cpf = str(input("Digite o número do CPF do Cliente: "))
                cliente = self.valida_cliente(cpf)
                if cliente == None:
                    return None
    
                # Lista as montadoras existentes para inserir no aluguel
                self.relatorio.get_relatorio_montadoras()
                cnpj = str(input("Digite o número do CNPJ da Montadora: "))
                montadora = self.valida_montadora(cnpj)
                if montadora == None:
                    return None
    
                data_hoje = date.today().strftime("%m-%d-%Y")
                quantDiasSomar = int(input("Insira numero de dias a ser adicionado a data de hoje para data_aluguel_final: "))
                data_amanha = (date.today() + datetime.timedelta(quantDiasSomar)).strftime("%m-%d-%Y")
    
                # Atualiza a descrição do veiculo existente
                self.mongo.db["alugueis"].update_one({"codigo_aluguel": codigo_aluguel}, 
                                                    {"$set": {"cnpj": f'{montadora.get_CNPJ()}',
                                                              "cpf":  f'{cliente.get_CPF()}',
                                                              "data_aluguel_inicial": data_hoje,
                                                              "data_aluguel_final": data_amanha,
                                                              }
                                                    })
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_aluguel = self.recupera_aluguel_codigo(codigo_aluguel)
                # Cria um novo objeto Veiculo
                aluguel_atualizado = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
                # Exibe os atributos do novo veiculo
                print(aluguel_atualizado.to_string())
                self.mongo.close()
                aux_loop = input("Deseja atualizar mais um aluguel? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                self.mongo.close()
                print(f"O código {codigo_aluguel} não existe.")
                aux_loop = input("Deseja tentar atualizar um veiculo novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_aluguel(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_aluguel = int(input("Código do Aluguel que irá excluir: "))        

            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_aluguel(codigo_aluguel):            
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_aluguel = self.recupera_aluguel_codigo(codigo_aluguel)
                cliente = self.valida_cliente(df_aluguel.cpf.values[0])
                montadora = self.valida_montadora(df_aluguel.cnpj.values[0])

                opcao_excluir = input(f"Tem certeza que deseja excluir o aluguel {codigo_aluguel} [S ou N]: ")
                if opcao_excluir.lower() == "s":
                    print("Atenção, caso o aluguel possua itens, também serão excluídos!")
                    opcao_excluir = input(f"Tem certeza que deseja excluir o aluguel {codigo_aluguel} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Revome o veiculo da tabela
                        self.mongo.db["itens_aluguel"].delete_one({"codigo_aluguel": codigo_aluguel})
                        print("Itens do aluguel removidos com sucesso!")
                        self.mongo.db["alugueis"].delete_one({"codigo_aluguel": codigo_aluguel})
                        # Cria um novo objeto Veiculo para informar que foi removido
                        aluguel_excluido = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
                        self.mongo.close()
                        # Exibe os atributos do veiculo excluído
                        print("Aluguel Removido com Sucesso!")
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
                self.mongo.close()
                print(f"O código {codigo_aluguel} não existe.")
                aux_loop = input("Deseja tentar excluir um aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_aluguel(self, codigo:int=None, external: bool = False) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = self.recupera_aluguel_codigo(codigo=codigo, external=external)
        return df_aluguel.empty

    def recupera_aluguel(self, _id:ObjectId=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = pd.DataFrame(list(self.mongo.db["alugueis"].find({"_id":_id}, {"codigo_aluguel": 1, "data_aluguel_inicial": 1, "data_aluguel_final": 1, "cpf": 1, "cnpj": 1, "_id": 0})))
        return df_aluguel

    def recupera_aluguel_codigo(self, codigo:int=None, external: bool = False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = pd.DataFrame(list(self.mongo.db["alugueis"].find({"codigo_aluguel": codigo}, {"codigo_aluguel": 1, "data_aluguel_inicial": 1, "data_aluguel_final": 1, "cpf": 1, "cnpj": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_aluguel

    def valida_cliente(self, cpf:str=None) -> Cliente:
        if self.ctrl_cliente.verifica_existencia_cliente(cpf=cpf, external=True):
            print(f"O CPF {cpf} informado não existe na base.")
            return None
        else:
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_cliente = self.ctrl_cliente.recupera_cliente(cpf=cpf, external=True)
            # Cria um novo objeto cliente
            cliente = Cliente(df_cliente.cpf.values[0], df_cliente.nome.values[0])
            return cliente

    def valida_montadora(self, cnpj:str=None) -> Montadora:
        if self.ctrl_montadora.verifica_existencia_montadora(cnpj, external=True):
            print(f"O CNPJ {cnpj} informado não existe na base.")
            return None
        else:
            # Recupera os dados do novo montadora criado transformando em um DataFrame
            df_montadora = self.ctrl_montadora.recupera_montadora(cnpj, external=True)
            # Cria um novo objeto montadora
            montadora = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
            return montadora
