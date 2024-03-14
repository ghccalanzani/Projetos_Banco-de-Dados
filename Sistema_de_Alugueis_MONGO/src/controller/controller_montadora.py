import pandas as pd
from model.montadoras import Montadora
from conexion.mongo_queries import MongoQueries

class Controller_Montadora:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_montadora(self) -> Montadora:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuario o novo CNPJ
            cnpj = input("CNPJ (Novo): ")

            if self.verifica_existencia_montadora(cnpj):
                # Solicita ao usuario a nova razão social
                razao_social = input("Razão Social (Novo): ")
                # Solicita ao usuario o novo nome fantasia
                nome_fantasia = input("Nome Fantasia (Novo): ")
                # Insere e persiste o novo montadora
                self.mongo.db["montadoras"].insert_one({"cnpj": cnpj, "razao_social": razao_social, "nome_fantasia": nome_fantasia})
                # Recupera os dados do novo montadora criado transformando em um DataFrame
                df_montadora = self.recupera_montadora(cnpj)
                # Cria um novo objeto montadora
                novo_montadora = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                # Exibe os atributos do novo montadora
                print(novo_montadora.to_string())
                self.mongo.close()
                aux_loop = input("Deseja inserir mais uma montadora? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                self.mongo.close()
                print(f"O CNPJ {cnpj} já está cadastrado.")
                aux_loop = input("Deseja tentar inserir uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def atualizar_montadora(self) -> Montadora:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuário o código do montadora a ser alterado
            cnpj = int(input("CNPJ da montadora que deseja atualizar: "))

            # Verifica se o montadora existe na base de dados
            if not self.verifica_existencia_montadora(cnpj):
                # Solicita ao usuario a nova razão social
                razao_social = input("Razão Social (Novo): ")
                # Solicita ao usuario o novo nome fantasia
                nome_fantasia = input("Nome Fantasia (Novo): ")            
                # Atualiza o nome do montadora existente
                self.mongo.db["montadoras"].update_one({"cnpj":f"{cnpj}"},{"$set": {"razao_social":razao_social, "nome_fantasia":nome_fantasia}})
                # Recupera os dados do novo montadora criado transformando em um DataFrame
                df_montadora = self.recupera_montadora(cnpj)
                # Cria um novo objeto montadora
                montadora_atualizado = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                # Exibe os atributos do novo montadora
                print(montadora_atualizado.to_string())
                self.mongo.close()
                aux_loop = input("Deseja atualizar mais uma montadora? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                self.mongo.close()
                print(f"O CNPJ {cnpj} não existe.")
                aux_loop = input("Deseja tentar atualizar uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_montadora(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuário o CPF do montadora a ser alterado
            cnpj = int(input("CNPJ da montadora que irá excluir: "))        

            # Verifica se o montadora existe na base de dados
            if not self.verifica_existencia_montadora(cnpj):            
                # Recupera os dados do novo montadora criado transformando em um DataFrame
                df_montadora = self.recupera_montadora(cnpj)
                if not self.verifica_existencia_montadora_em_alugueis(cnpj):
                    print("A montadora possui alugueis pendentes. Não é possível excluir!")
                    aux_loop = input("Deseja tentar excluir outra montadora? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
                else:
                    opcao_excluir = input(f"Tem certeza que deseja excluir a montadora de CNPJ {cnpj} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Revome o montadora da tabela
                        self.mongo.db["montadoras"].delete_one({"cnpj":f"{cnpj}"})
                        # Cria um novo objeto montadora para informar que foi removido
                        montadora_excluido = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                        self.mongo.close()
                        # Exibe os atributos do montadora excluído
                        print("Montadora Removida com Sucesso!")
                        print(montadora_excluido.to_string())
                        aux_loop = input("Deseja excluir mais uma montadora? (S ou N)\n").lower()
                        aux_Skip = aux_loop
                        if aux_loop == "n":
                            break;
                    if aux_Skip != "s":
                        aux_loop = input("Ainda deseja excluir uma montadora? (S ou N)\n").lower()
                        if aux_loop == "n":
                            break;
            else:
                self.mongo.close()
                print(f"O CNPJ {cnpj} não existe.")
                aux_loop = input("Deseja tentar excluir uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_montadora(self, cnpj:str=None, external:bool=False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo montadora criado transformando em um DataFrame
        df_montadora = pd.DataFrame(self.mongo.db["montadoras"].find({"cnpj":f"{cnpj}"}, {"cnpj": 1, "razao_social": 1, "nome_fantasia": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_montadora.empty

    def verifica_existencia_montadora_em_alugueis(self, cnpj:str=None, external:bool=False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo montadora criado transformando em um DataFrame
        df_montadora = pd.DataFrame(self.mongo.db["alugueis"].find({"cnpj":f"{cnpj}"}, {"cnpj": 1, "razao_social": 1, "nome_fantasia": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_montadora.empty

    def recupera_montadora(self, cnpj:str=None, external:bool=False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo cliente criado transformando em um DataFrame
        df_cliente = pd.DataFrame(list(self.mongo.db["montadoras"].find({"cnpj":f"{cnpj}"}, {"cnpj": 1, "razao_social": 1, "nome_fantasia": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_cliente