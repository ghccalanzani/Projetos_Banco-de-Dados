import pandas as pd
from bson import ObjectId

from reports.relatorios import Relatorio

from model.itens_aluguel import ItemAluguel
from model.veiculos import Veiculo
from model.alugueis import Aluguel

from controller.controller_veiculo import Controller_Veiculo
from controller.controller_aluguel import Controller_Aluguel

from conexion.mongo_queries import MongoQueries

class Controller_Item_Aluguel:
    def __init__(self):
        self.ctrl_veiculo = Controller_Veiculo()
        self.ctrl_aluguel = Controller_Aluguel()
        self.mongo = MongoQueries()
        self.relatorio = Relatorio()
        
    def inserir_item_aluguel(self) -> ItemAluguel:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco
            self.mongo.connect()

            # Lista os aluguel existentes para inserir no item de aluguel
            self.relatorio.get_relatorio_alugueis()
            codigo_aluguel = int(str(input("Digite o número do Aluguel: ")))
            aluguel = self.valida_aluguel(codigo_aluguel)
            if aluguel == None:
                return None

            # Lista os veiculos existentes para inserir no item de aluguel
            self.relatorio.get_relatorio_veiculos()
            codigo_veiculo = int(str(input("Digite o código do Veiculo: ")))
            veiculo = self.valida_veiculo(codigo_veiculo)
            if veiculo == None:
                return None

            # Solicita a quantidade de itens do aluguel para o veiculo selecionado
            quantidade = float(input(f"Informe a quantidade de itens do veiculo {veiculo.get_modelo_veiculo()}: "))
            # Solicita o valor unitário do veiculo selecionado
            valor_aluguel_veiculo = float(input(f"Informe o valor do aluguel do veiculo {veiculo.get_modelo_veiculo()}: "))

            proximo_item_aluguel = self.mongo.db["itens_aluguel"].aggregate([
                                                        {
                                                            '$group': {
                                                                '_id': '$itens_aluguel', 
                                                                'proximo_item_aluguel': {
                                                                    '$max': '$codigo_item_aluguel'
                                                                }
                                                            }
                                                        }, {
                                                            '$project': {
                                                                'proximo_item_aluguel': {
                                                                    '$sum': [
                                                                        '$proximo_item_aluguel', 1
                                                                    ]
                                                                }, 
                                                                '_id': 0
                                                            }
                                                        }
                                                    ])

            proximo_item_aluguel = int(list(proximo_item_aluguel)[0]['proximo_item_aluguel'])
            # Cria um dicionário para mapear as variáveis de entrada e saída
            data = dict(codigo_item_aluguel=proximo_item_aluguel, valor_aluguel_veiculo=valor_aluguel_veiculo, quantidade=quantidade, codigo_aluguel=int(aluguel.get_codigo_aluguel()), codigo_veiculo=int(veiculo.get_codigo_veiculo()))
            # Insere e Recupera o código do novo item de aluguel
            id_item_aluguel = self.mongo.db["itens_aluguel"].insert_one(data)
            # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
            df_item_aluguel = self.recupera_item_aluguel(id_item_aluguel.inserted_id)
            # Cria um novo objeto Item de Aluguel
            novo_item_aluguel = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], df_item_aluguel.quantidade.values[0], df_item_aluguel.valor_aluguel_veiculo.values[0], aluguel, veiculo)
            # Exibe os atributos do novo Item de Aluguel
            print(novo_item_aluguel.to_string())
            self.mongo.close()
            aux_loop = input("Deseja inserir mais um item de aluguel? (S ou N)\n").lower()
            if aux_loop == "n":
                break;

    def atualizar_item_aluguel(self) -> ItemAluguel:
        aux_loop = "s"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuário o código do item de aluguel a ser alterado
            codigo_item_aluguel = int(input("Código do Item de Aluguel que irá alterar: "))        

            # Verifica se o item de aluguel existe na base de dados
            if not self.verifica_existencia_item_aluguel(codigo_item_aluguel):

                # Lista os aluguel existentes para inserir no item de aluguel
                self.relatorio.get_relatorio_alugueis()
                codigo_aluguel = int(str(input("Digite o número do Aluguel: ")))
                aluguel = self.valida_aluguel(codigo_aluguel)
                if aluguel == None:
                    return None

                # Lista os veiculos existentes para inserir no item de aluguel
                self.relatorio.get_relatorio_veiculos()
                codigo_veiculo = int(str(input("Digite o código do Veiculo: ")))
                veiculo = self.valida_veiculo(codigo_veiculo)
                if veiculo == None:
                    return None

                # Solicita a quantidade de itens do aluguel para o veiculo selecionado
                quantidade = float(input(f"Informe a quantidade de itens do veiculo {veiculo.get_modelo_veiculo()}: "))
                # Solicita o valor unitário do veiculo selecionado
                valor_aluguel_veiculo = float(input(f"Informe o valor unitário do veiculo {veiculo.get_modelo_veiculo()}: "))

                # Atualiza o item de aluguel existente
                self.mongo.db["itens_aluguel"].update_one({"codigo_item_aluguel": codigo_item_aluguel},
                                                         {"$set": {"quantidade": quantidade,
                                                                   "valor_aluguel_veiculo":  valor_aluguel_veiculo,
                                                                   "codigo_aluguel": int(aluguel.get_codigo_aluguel()),
                                                                   "codigo_veiculo": int(veiculo.get_codigo_veiculo())
                                                              }
                                                         })
                # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
                df_item_aluguel = self.recupera_item_aluguel_codigo(codigo_item_aluguel)
                # Cria um novo objeto Item de Aluguel
                item_aluguel_atualizado = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], df_item_aluguel.quantidade.values[0], df_item_aluguel.valor_aluguel_veiculo.values[0], aluguel, veiculo)
                # Exibe os atributos do item de aluguel
                print(item_aluguel_atualizado.to_string())
                self.mongo.close()
                aux_loop = input("Deseja atualizar mais um item de aluguel? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                self.mongo.close()
                print(f"O código {codigo_item_aluguel} não existe.")
                aux_loop = input("Deseja tentar atualizar um item de aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_item_aluguel(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

            # Solicita ao usuário o código do item de aluguel a ser alterado
            codigo_item_aluguel = int(input("Código do Item de Aluguel que irá excluir: "))        

            # Verifica se o item de aluguel existe na base de dados
            if not self.verifica_existencia_item_aluguel(codigo_item_aluguel):            
                # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
                df_item_aluguel = self.recupera_item_aluguel_codigo(codigo_item_aluguel)
                aluguel = self.valida_aluguel(int(df_item_aluguel.codigo_aluguel.values[0]))
                veiculo = self.valida_veiculo(int(df_item_aluguel.codigo_veiculo.values[0]))

                opcao_excluir = input(f"Tem certeza que deseja excluir o item de aluguel {codigo_item_aluguel} [S ou N]: ")
                if opcao_excluir.lower() == "s":
                    # Revome o item de aluguel da tabela
                    self.mongo.db["itens_aluguel"].delete_one({"codigo_item_aluguel": codigo_item_aluguel})
                    # Cria um novo objeto Item de Aluguel para informar que foi removido
                    item_aluguel_excluido = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], 
                                                      df_item_aluguel.quantidade.values[0], 
                                                      df_item_aluguel.valor_aluguel_veiculo.values[0], 
                                                      aluguel, 
                                                      veiculo)
                    self.mongo.close()
                    # Exibe os atributos do veiculo excluído
                    print("Item do Aluguel Removido com Sucesso!")
                    print(item_aluguel_excluido.to_string())
                    aux_loop = input("Deseja excluir mais um item de aluguel? (S ou N)\n").lower()
                    aux_Skip = aux_loop
                    if aux_loop == "n":
                        break;
                if aux_Skip != "s":
                    aux_loop = input("Ainda deseja excluir um item de aluguel? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
            else:
                self.mongo.close()
                print(f"O código {codigo_item_aluguel} não existe.")
                aux_loop = input("Deseja tentar excluir um item de aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_item_aluguel(self, codigo:int=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = self.recupera_item_aluguel_codigo(codigo=codigo)
        return df_aluguel.empty

    def recupera_item_aluguel(self, _id:ObjectId=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = pd.DataFrame(list(self.mongo.db["itens_aluguel"].find({"_id": _id}, {"codigo_item_aluguel":1, "quantidade": 1, "valor_aluguel_veiculo": 1, "codigo_aluguel": 1, "codigo_veiculo": 1, "_id": 0})))
        return df_aluguel

    def recupera_item_aluguel_codigo(self, codigo:int=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = pd.DataFrame(list(self.mongo.db["itens_aluguel"].find({"codigo_item_aluguel": codigo}, {"codigo_item_aluguel":1, 
                                                                                                          "quantidade": 1, 
                                                                                                          "valor_aluguel_veiculo": 1, 
                                                                                                          "codigo_aluguel": 1, 
                                                                                                          "codigo_veiculo": 1, 
                                                                                                          "_id": 0})))
        return df_aluguel

    def valida_aluguel(self, codigo_aluguel:int=None) -> Aluguel:
        if self.ctrl_aluguel.verifica_existencia_aluguel(codigo_aluguel, external=True):
            print(f"O aluguel {codigo_aluguel} informado não existe na base.")
            return None
        else:
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_aluguel = self.ctrl_aluguel.recupera_aluguel_codigo(codigo_aluguel, external=True)
            cliente = self.ctrl_aluguel.valida_cliente(df_aluguel.cpf.values[0])
            montadora = self.ctrl_aluguel.valida_montadora(df_aluguel.cnpj.values[0])
            # Cria um novo objeto cliente
            aluguel = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
            return aluguel

    def valida_veiculo(self, codigo_veiculo:int=None) -> Veiculo:
        if self.ctrl_veiculo.verifica_existencia_veiculo(codigo_veiculo, external=True):
            print(f"O veiculo {codigo_veiculo} informado não existe na base.")
            return None
        else:
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = self.ctrl_veiculo.recupera_veiculo_codigo(codigo_veiculo, external=True)
            # Cria um novo objeto Veiculo
            veiculo = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
            return veiculo