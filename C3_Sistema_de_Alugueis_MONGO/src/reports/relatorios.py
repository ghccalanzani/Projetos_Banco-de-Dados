from conexion.mongo_queries import MongoQueries
import pandas as pd
from pymongo import ASCENDING, DESCENDING

class Relatorio:
    def __init__(self):
        pass

    def get_relatorio_alugueis_e_itens(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db.alugueis.aggregate([{
                                                    "$lookup":{"from":"itens_aluguel",
                                                               "localField":"codigo_aluguel",
                                                               "foreignField":"codigo_aluguel",
                                                               "as":"item"
                                                              }
                                                   },
                                                   {
                                                    "$unwind": { "path": "$item"}
                                                   },
                                                   {
                                                    "$lookup":{"from":"clientes",
                                                               "localField":"cpf",
                                                               "foreignField":"cpf",
                                                               "as":"cliente"
                                                              }
                                                   },
                                                   {
                                                    "$unwind": { "path": "$cliente" }
                                                   },
                                                   {
                                                    "$lookup":{"from":"montadoras",
                                                               "localField":"cnpj",
                                                               "foreignField":"cnpj",
                                                               "as":"montadora"
                                                              }
                                                   },
                                                   {
                                                    "$unwind": {"path": "$montadora"}
                                                   },
                                                   {
                                                    "$lookup":{"from":'veiculos',
                                                               "localField":"item.codigo_veiculo",
                                                               "foreignField":"codigo_veiculo",
                                                               "as":"veiculo"
                                                              }
                                                   },
                                                   {
                                                    "$unwind": {"path": "$veiculo"}
                                                   },
                                                   {
                                                    "$project": {"codigo_aluguel": 1,
                                                                 "codigo_item_aluguel": "$item.codigo_item_aluguel",
                                                                 "cliente": "$cliente.nome",
                                                                 "data_aluguel_inicial":1,
                                                                 "data_aluguel_final":1,
                                                                 "montadora": "$montadora.razao_social",
                                                                 "veiculo": "$veiculo.modelo_veiculo",
                                                                 "veiculo": "$veiculo.cor_veiculo",
                                                                 "veiculo": "$veiculo.tipo_combustivel",
                                                                 "quantidade": "$item.quantidade",
                                                                 "valor_aluguel_veiculo": "$item.valor_aluguel_veiculo",
                                                                 "receita_diaria": {'$multiply':['$item.quantidade','$item.valor_aluguel_veiculo']},
                                                                 "_id": 0
                                                                }
                                                   }])
        
        df_alugueis_itens = pd.DataFrame(list(query_result))
        # Fecha a conexão com o Mongo
        mongo.close()
        # Exibe o resultado
        print(df_alugueis_itens)
        input("Pressione Enter para Sair do Relatório de Alugueis")

    def get_relatorio_alugueis_por_montadora(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db["alugueis"].aggregate([
                                                    {
                                                        '$group': {
                                                            '_id': '$cnpj', 
                                                            'qtd_alugueis': {
                                                                '$sum': 1
                                                            }
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'cnpj': '$_id', 
                                                            'qtd_alugueis': 1, 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'alugueis', 
                                                            'localField': 'cnpj', 
                                                            'foreignField': 'cnpj', 
                                                            'as': 'aluguel'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$aluguel'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'cnpj': 1, 
                                                            'qtd_alugueis': 1, 
                                                            'aluguel': '$aluguel.codigo_aluguel', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'itens_aluguel', 
                                                            'localField': 'aluguel', 
                                                            'foreignField': 'codigo_aluguel', 
                                                            'as': 'item'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$item'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'cnpj': 1, 
                                                            'qtd_alugueis': 1, 
                                                            'quantidade': '$item.quantidade', 
                                                            'valor_aluguel_veiculo': '$item.valor_aluguel_veiculo', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$group': {
                                                            '_id': {
                                                                'cnpj': '$cnpj', 
                                                                'qtd_alugueis': '$qtd_alugueis'
                                                            }, 
                                                            'receita_diaria': {
                                                                '$sum': {
                                                                    '$multiply': [
                                                                        '$quantidade', '$valor_aluguel_veiculo'
                                                                    ]
                                                                }
                                                            }
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$_id'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'cnpj': '$_id.cnpj', 
                                                            'qtd_alugueis': '$_id.qtd_alugueis', 
                                                            'receita_diaria': '$receita_diaria', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'montadoras', 
                                                            'localField': 'cnpj', 
                                                            'foreignField': 'cnpj', 
                                                            'as': 'montadora'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$montadora'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'empresa': '$montadora.nome_fantasia', 
                                                            'qtd_alugueis': 1, 
                                                            'receita_diaria': 1, 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$sort': {
                                                            'empresa': 1
                                                        }
                                                    }
                                                ])
        df_alugueis_montadora = pd.DataFrame(list(query_result))
        # Fecha a conexão com o Mongo
        mongo.close()
        # Exibe o resultado
        print(df_alugueis_montadora[["empresa", "qtd_alugueis", "receita_diaria"]])
        input("Pressione Enter para Sair do Relatório de Montadoras")

    def get_relatorio_veiculos(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db["veiculos"].find({}, 
                                                 {"codigo_veiculo": 1, 
                                                  "modelo_veiculo": 1,
                                                  "cor_veiculo": 1,
                                                  "tipo_combustivel": 1, 
                                                  "_id": 0
                                                 }).sort("modelo_veiculo", ASCENDING)
        df_veiculo = pd.DataFrame(list(query_result))
        # Fecha a conexão com o Mongo
        mongo.close()
        # Exibe o resultado
        print(df_veiculo)        
        input("Pressione Enter para Sair do Relatório de Veiculos")

    def get_relatorio_clientes(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db["clientes"].find({}, 
                                                 {"cpf": 1, 
                                                  "nome": 1, 
                                                  "_id": 0
                                                 }).sort("nome", ASCENDING)
        df_cliente = pd.DataFrame(list(query_result))
        # Fecha a conexão com o mongo
        mongo.close()
        # Exibe o resultado
        print(df_cliente)
        input("Pressione Enter para Sair do Relatório de Clientes")

    def get_relatorio_montadoras(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db["montadoras"].find({}, 
                                                     {"cnpj": 1, 
                                                      "razao_social": 1, 
                                                      "nome_fantasia": 1, 
                                                      "_id": 0
                                                     }).sort("nome_fantasia", ASCENDING)
        df_montadora = pd.DataFrame(list(query_result))
        # Fecha a conexão com o mongo
        mongo.close()
        # Exibe o resultado
        print(df_montadora)        
        input("Pressione Enter para Sair do Relatório de Montadoras")

    def get_relatorio_alugueis(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Recupera os dados transformando em um DataFrame
        query_result = mongo.db["alugueis"].aggregate([
                                                    {
                                                        '$lookup': {
                                                            'from': 'montadoras', 
                                                            'localField': 'cnpj', 
                                                            'foreignField': 'cnpj', 
                                                            'as': 'montadora'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$montadora'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'codigo_aluguel': 1, 
                                                            'data_aluguel_inicial': 1,
                                                            'data_aluguel_final': 1,  
                                                            'empresa': '$montadora.nome_fantasia', 
                                                            'cpf': 1, 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'clientes', 
                                                            'localField': 'cpf', 
                                                            'foreignField': 'cpf', 
                                                            'as': 'cliente'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$cliente'
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'codigo_aluguel': 1, 
                                                            'data_aluguel_inicial': 1,
                                                            'data_aluguel_final': 1, 
                                                            'empresa': 1, 
                                                            'cliente': '$cliente.nome', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'itens_aluguel', 
                                                            'localField': 'codigo_aluguel', 
                                                            'foreignField': 'codigo_aluguel', 
                                                            'as': 'item'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$item', 'preserveNullAndEmptyArrays': True
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'codigo_aluguel': 1, 
                                                            'data_aluguel_inicial': 1,
                                                            'data_aluguel_final': 1, 
                                                            'empresa': 1, 
                                                            'cliente': 1, 
                                                            'item_aluguel': '$item.codigo_item_aluguel', 
                                                            'quantidade': '$item.quantidade', 
                                                            'valor_aluguel_veiculo': '$item.valor_aluguel_veiculo', 
                                                            'receita_diaria': {
                                                                '$multiply': [
                                                                    '$item.quantidade', '$item.valor_aluguel_veiculo'
                                                                ]
                                                            }, 
                                                            'codigo_veiculo': '$item.codigo_veiculo', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$lookup': {
                                                            'from': 'veiculos', 
                                                            'localField': 'codigo_veiculo', 
                                                            'foreignField': 'codigo_veiculo', 
                                                            'as': 'veiculo'
                                                        }
                                                    }, {
                                                        '$unwind': {
                                                            'path': '$veiculo', 'preserveNullAndEmptyArrays': True
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'codigo_aluguel': 1, 
                                                            'data_aluguel_inicial': 1,
                                                            'data_aluguel_final': 1, 
                                                            'empresa': 1, 
                                                            'cliente': 1, 
                                                            'item_aluguel': 1, 
                                                            'quantidade': 1, 
                                                            'valor_aluguel_veiculo': 1, 
                                                            'receita_diaria': 1, 
                                                            'veiculo': '$veiculo.modelo_veiculo', 
                                                            '_id': 0
                                                        }
                                                    }, {
                                                        '$sort': {
                                                            'cliente': 1,
                                                            'item_aluguel': 1
                                                        }
                                                    }
                                                ])
        df_aluguel = pd.DataFrame(list(query_result))
        # Fecha a conexão com o Mongo
        mongo.close()
        print(df_aluguel[["codigo_aluguel", "data_aluguel_inicial", "data_aluguel_final", "cliente", "empresa", "item_aluguel", "veiculo", "quantidade", "valor_aluguel_veiculo", "receita_diaria"]])
        input("Pressione Enter para Sair do Relatório de Alugueis")
    
    def get_relatorio_itens_alugueis(self):
        # Cria uma nova conexão com o banco
        mongo = MongoQueries()
        mongo.connect()
        # Realiza uma consulta no mongo e retorna o cursor resultante para a variável
        query_result = mongo.db['itens_aluguel'].aggregate([{
                                                            '$lookup':{'from':'veiculos',
                                                                       'localField':'codigo_veiculo',
                                                                       'foreignField':'codigo_veiculo',
                                                                       'as':'veiculo'
                                                                      }
                                                           },
                                                           {
                                                            '$unwind':{"path": "$veiculo"}
                                                           },
                                                           {'$project':{'codigo_aluguel':1, 
                                                                        'codigo_item_aluguel':1,
                                                                    'codigo_veiculo':'$veiculo.codigo_veiculo',
                                                                    'modelo_veiculo':'$veiculo.modelo_veiculo',
                                                                    'cor_veiculo':'$veiculo.cor_veiculo',
                                                                    'tipo_combustivel':'$veiculo.tipo_combustivel',
                                                                    'quantidade':1,
                                                                    'valor_aluguel_veiculo':1,
                                                                    'receita_diaria':{'$multiply':['$quantidade','$valor_aluguel_veiculo']},
                                                                    '_id':0
                                                                    }}
                                                          ])
        # Converte o cursos em lista e em DataFrame
        df_itens_aluguel = pd.DataFrame(list(query_result))
        # Troca o tipo das colunas
        df_itens_aluguel.codigo_item_aluguel = df_itens_aluguel.codigo_item_aluguel.astype(int)
        df_itens_aluguel.codigo_aluguel = df_itens_aluguel.codigo_aluguel.astype(int)
        # Fecha a conexão com o mongo
        mongo.close()
        # Exibe o resultado
        print(df_itens_aluguel[["codigo_aluguel", "codigo_item_aluguel", "codigo_veiculo", "modelo_veiculo", "cor_veiculo", "tipo_combustivel", "quantidade", "valor_aluguel_veiculo", "receita_diaria"]])
        input("Pressione Enter para Sair do Relatório de Itens de Alugueis")