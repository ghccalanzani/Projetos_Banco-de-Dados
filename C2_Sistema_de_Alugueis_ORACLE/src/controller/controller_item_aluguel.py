from model.itens_aluguel import ItemAluguel
from model.veiculos import Veiculo
from controller.controller_veiculo import Controller_Veiculo
from model.alugueis import Aluguel
from controller.controller_aluguel import Controller_Aluguel
from model.montadoras import Montadora
from controller.controller_montadora import Controller_Montadora
from conexion.oracle_queries import OracleQueries

class Controller_Item_Aluguel:
    def __init__(self):
        self.ctrl_veiculo = Controller_Veiculo()
        self.ctrl_aluguel = Controller_Aluguel()
        self.ctrl_montadora = Controller_Montadora()
        
    def inserir_item_aluguel(self) -> ItemAluguel:
        aux_loop = "s"
        while aux_loop == "s":
            ''' Ref.: https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#anonymous-pl-sql-blocks'''

            # Cria uma nova conexão com o banco
            oracle = OracleQueries()

            # Lista os alugueis existentes para inserir no item de aluguel
            self.listar_alugueis(oracle, need_connect=True)
            codigo_aluguel = str(input("Digite o número do Aluguel: "))
            aluguel = self.valida_aluguel(oracle, codigo_aluguel)
            if aluguel == None:
                return None

            # Lista os veiculos existentes para inserir no item de aluguel
            self.listar_veiculos(oracle, need_connect=True)
            codigo_veiculo = str(input("Digite o código do Veiculo: "))
            veiculo = self.valida_veiculo(oracle, codigo_veiculo)
            if veiculo == None:
                return None

            # Solicita a quantidade de itens do aluguel para o veiculo selecionado
            quantidade = float(input(f"Informe a quantidade do veiculo {veiculo.get_modelo_veiculo()}: "))
            # Solicita o valor do aluguel do veiculo selecionado
            valor_aluguel_veiculo = float(input(f"Informe o valor do aluguel do veiculo {veiculo.get_modelo_veiculo()}: "))

            # Recupera o cursor para executar um bloco PL/SQL anônimo
            cursor = oracle.connect()
            # Cria a variável de saída com o tipo especificado
            output_value = cursor.var(int)

            # Cria um dicionário para mapear as variáveis de entrada e saída
            data = dict(codigo=output_value, quantidade=quantidade, valor_aluguel_veiculo=valor_aluguel_veiculo, codigo_aluguel=int(aluguel.get_codigo_aluguel()), codigo_veiculo=int(veiculo.get_codigo_veiculo()))
            # Executa o bloco PL/SQL anônimo para inserção do novo item de aluguel e recuperação da chave primária criada pela sequence
            cursor.execute("""
            begin
                :codigo := ITENS_ALUGUEL_CODIGO_ITEM_SEQ.NEXTVAL;
                insert into itens_aluguel values(:codigo, :quantidade, :valor_aluguel_veiculo, :codigo_aluguel, :codigo_veiculo);
            end;
            """, data)
            # Recupera o código do novo item do aluguel
            codigo_item_aluguel = output_value.getvalue()
            # Persiste (confirma) as alterações
            oracle.conn.commit()
            # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
            df_item_aluguel = oracle.sqlToDataFrame(f"select codigo_item_aluguel, quantidade, valor_aluguel_veiculo, codigo_aluguel, codigo_veiculo from itens_aluguel where codigo_item_aluguel = {codigo_item_aluguel}")
            # Cria um novo objeto Item de Aluguel
            novo_item_aluguel = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], df_item_aluguel.quantidade.values[0], df_item_aluguel.valor_aluguel_veiculo.values[0], aluguel, veiculo)
            # Exibe os atributos do novo Item de Aluguel
            print(novo_item_aluguel.to_string())
            aux_loop = input("Deseja inserir mais um item de aluguel? (S ou N)\n").lower()
            if aux_loop == "n":
                break;

    def atualizar_item_aluguel(self) -> ItemAluguel:
        aux_loop = "s"
        while aux_loop == "s":    
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do item de aluguel a ser alterado
            codigo_item_aluguel = int(input("Código do Item de Aluguel que irá alterar: "))        

            # Verifica se o item de aluguel existe na base de dados
            if not self.verifica_existencia_item_aluguel(oracle, codigo_item_aluguel):

                # Lista os aluguel existentes para inserir no item de aluguel
                self.listar_alugueis(oracle, need_connect=True)
                codigo_aluguel = str(input("Digite o número do Aluguel: "))
                aluguel = self.valida_aluguel(oracle, codigo_aluguel)
                if aluguel == None:
                    return None

                # Lista os veiculos existentes para inserir no item de aluguel
                self.listar_veiculos(oracle, need_connect=True)
                codigo_veiculo = str(input("Digite o código do Veiculo: "))
                veiculo = self.valida_veiculo(oracle, codigo_veiculo)
                if veiculo == None:
                    return None

                # Solicita a quantidade de itens do aluguel para o veiculo selecionado
                quantidade = float(input(f"Informe a quantidade de itens do veiculo {veiculo.get_modelo_veiculo()}: "))
                # Solicita o valor do aluguel do veiculo selecionado
                valor_aluguel_veiculo = float(input(f"Informe o valor do aluguel do veiculo {veiculo.get_modelo_veiculo()}: "))

                # Atualiza o item de aluguel existente
                oracle.write(f"update itens_aluguel set quantidade = {quantidade}, valor_aluguel_veiculo = {valor_aluguel_veiculo}, codigo_aluguel = {aluguel.get_codigo_aluguel()}, codigo_veiculo = {veiculo.get_codigo_veiculo()} where codigo_item_aluguel = {codigo_item_aluguel}")
                # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
                df_item_aluguel = oracle.sqlToDataFrame(f"select codigo_item_aluguel, quantidade, valor_aluguel_veiculo, codigo_aluguel, codigo_veiculo from itens_aluguel where codigo_item_aluguel = {codigo_item_aluguel}")
                # Cria um novo objeto Item de Aluguel
                item_aluguel_atualizado = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], df_item_aluguel.quantidade.values[0], df_item_aluguel.valor_aluguel_veiculo.values[0], aluguel, veiculo)
                # Exibe os atributos do item de aluguel
                print(item_aluguel_atualizado.to_string())
                aux_loop = input("Deseja atualizar mais um item de aluguel? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O código {codigo_item_aluguel} não existe.")
                aux_loop = input("Deseja tentar atualizar um item de aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_item_aluguel(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":    
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do item de aluguel a ser alterado
            codigo_item_aluguel = int(input("Código do Item de Aluguel que irá excluir: "))        

            # Verifica se o item de aluguel existe na base de dados
            if not self.verifica_existencia_item_aluguel(oracle, codigo_item_aluguel):            
                # Recupera os dados do novo item de aluguel criado transformando em um DataFrame
                df_item_aluguel = oracle.sqlToDataFrame(f"select codigo_item_aluguel, quantidade, valor_aluguel_veiculo, codigo_aluguel, codigo_veiculo from itens_aluguel where codigo_item_aluguel = {codigo_item_aluguel}")
                aluguel = self.valida_aluguel(oracle, df_item_aluguel.codigo_aluguel.values[0])
                veiculo = self.valida_veiculo(oracle, df_item_aluguel.codigo_veiculo.values[0])

                opcao_excluir = input(f"Tem certeza que deseja excluir o item de aluguel {codigo_item_aluguel} [S ou N]: ")
                if opcao_excluir.lower() == "s":
                    # Remove o veiculo da tabela
                    oracle.write(f"delete from itens_aluguel where codigo_item_aluguel = {codigo_item_aluguel}")                
                    # Cria um novo objeto Item de Aluguel para informar que foi removido
                    item_aluguel_excluido = ItemAluguel(df_item_aluguel.codigo_item_aluguel.values[0], df_item_aluguel.quantidade.values[0], df_item_aluguel.valor_aluguel_veiculo.values[0], aluguel, veiculo)
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
                print(f"O código {codigo_item_aluguel} não existe.")
                aux_loop = input("Deseja tentar excluir um item de aluguel novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_item_aluguel(self, oracle:OracleQueries, codigo:int=None) -> bool:
        # Recupera os dados do novo aluguel criado transformando em um DataFrame
        df_aluguel = oracle.sqlToDataFrame(f"select codigo_item_aluguel, quantidade, valor_aluguel_veiculo, codigo_aluguel, codigo_veiculo from itens_aluguel where codigo_item_aluguel = {codigo}")
        return df_aluguel.empty

    def listar_alugueis(self, oracle:OracleQueries, need_connect:bool=False):
        query = """
                select a.codigo_aluguel
                    , a.data_aluguel_inicial
                    , a.data_aluguel_final
                    , (a.data_aluguel_final - a.data_aluguel_inicial) as quant_dias
                    , ((a.data_aluguel_final - a.data_aluguel_inicial) * valor_aluguel_veiculo) as valor_total
                    , c.nome as cliente
                    , nvl(m.nome_fantasia, m.razao_social) as empresa
                    , i.codigo_item_aluguel as item_aluguel
                    , vei.modelo_veiculo as veiculo
                    , i.quantidade as quant_veiculos_montadora
                    , i.valor_aluguel_veiculo
                from alugueis a
                inner join clientes c
                on a.cpf = c.cpf
                inner join montadoras m
                on a.cnpj = m.cnpj
                left join itens_aluguel i
                on a.codigo_aluguel = i.codigo_aluguel
                left join veiculos vei
                on i.codigo_veiculo = vei.codigo_veiculo
                order by c.nome
                """
        if need_connect:
            oracle.connect()
        print(oracle.sqlToDataFrame(query))

    def listar_veiculos(self, oracle:OracleQueries, need_connect:bool=False):
        query = """
                select vei.codigo_veiculo
                    , vei.modelo_veiculo
                    , vei.cor_veiculo
                    , vei.tipo_combustivel
                from veiculos vei
                order by vei.modelo_veiculo
                """
        if need_connect:
            oracle.connect()
        print(oracle.sqlToDataFrame(query))

    def valida_aluguel(self, oracle:OracleQueries, codigo_aluguel:int=None) -> Aluguel:
        if self.ctrl_aluguel.verifica_existencia_aluguel(oracle, codigo_aluguel):
            print(f"O aluguel {codigo_aluguel} informado não existe na base.")
            return None
        else:
            oracle.connect()
            # Recupera os dados do novo cliente criado transformando em um DataFrame
            df_aluguel = oracle.sqlToDataFrame(f"select codigo_aluguel, data_aluguel_inicial, data_aluguel_final, cpf, cnpj from alugueis where codigo_aluguel = {codigo_aluguel}")
            cliente = self.ctrl_aluguel.valida_cliente(oracle, df_aluguel.cpf.values[0])
            montadora = self.ctrl_aluguel.valida_montadora(oracle, df_aluguel.cnpj.values[0])
            # Cria um novo objeto cliente
            aluguel = Aluguel(df_aluguel.codigo_aluguel.values[0], df_aluguel.data_aluguel_inicial.values[0], df_aluguel.data_aluguel_final.values[0], cliente, montadora)
            return aluguel

    def valida_veiculo(self, oracle:OracleQueries, codigo_veiculo:int=None) -> Veiculo:
        if self.ctrl_veiculo.verifica_existencia_veiculo(oracle, codigo_veiculo):
            print(f"O veiculo {codigo_veiculo} informado não existe na base.")
            return None
        else:
            oracle.connect()
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo, modelo_veiculo, cor_veiculo, tipo_combustivel from veiculos where codigo_veiculo = {codigo_veiculo}")
            # Cria um novo objeto Veiculo
            veiculo = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0]) 
            return veiculo
