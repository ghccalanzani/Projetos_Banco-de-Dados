from model.veiculos import Veiculo
from conexion.oracle_queries import OracleQueries

class Controller_Veiculo:
    def __init__(self):
        pass
        
    def inserir_veiculo(self) -> Veiculo:
        aux_loop = "s"
        while aux_loop == "s":
            ''' Ref.: https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#anonymous-pl-sql-blocks'''

            # Cria uma nova conexão com o banco
            oracle = OracleQueries()
            # Recupera o cursos para executar um bloco PL/SQL anônimo
            cursor = oracle.connect()
            # Cria a variável de saída com o tipo especificado
            output_value = cursor.var(int)

            #Solicita ao usuario o modelo , cor e tipo do combustível do veiculo
            modelo_novo_veiculo = input("Modelo (Novo): ")
            cor_novo_veiculo = input("Cor (Novo): ")
            tipo_combustivel_novo = input("Tipo Combustível (Novo): ")

            # Cria um dicionário para mapear as variáveis de entrada e saída
            data = dict(codigo=output_value, modelo_veiculo=modelo_novo_veiculo, cor_veiculo=cor_novo_veiculo, tipo_combustivel=tipo_combustivel_novo)
            # Executa o bloco PL/SQL anônimo para inserção do novo veiculo e recuperação da chave primária criada pela sequence
            cursor.execute("""
            begin
                :codigo := VEICULOS_CODIGO_VEICULO_SEQ.NEXTVAL;
                insert into veiculos values(:codigo, :modelo_veiculo, :cor_veiculo, :tipo_combustivel);
            end;
            """, data)
            # Recupera o código do novo veiculo
            codigo_veiculo = output_value.getvalue()
            # Persiste (confirma) as alterações
            oracle.conn.commit()
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo, modelo_veiculo, cor_veiculo, tipo_combustivel from veiculos where codigo_veiculo = {codigo_veiculo}")
            # Cria um novo objeto Veiculo
            novo_veiculo = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
            # Exibe os atributos do novo veiculo
            print(novo_veiculo.to_string())
            aux_loop = input("Deseja inserir mais um veiculo? (S ou N)\n").lower()
            if aux_loop == "n":
                break;

    def atualizar_veiculo(self) -> Veiculo:
        aux_loop = "s"
        while aux_loop == "s":            
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_veiculo = int(input("Código do Veiculo que irá alterar: "))        

            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_veiculo(oracle, codigo_veiculo):
                # Solicita o novo modelo , cor e tipo do combustível  do veiculo
                novo_modelo_veiculo = input("Modelo (Novo): ")
                nova_cor_veiculo = input("Cor (Novo): ")
                novo_tipo_combustivel = input("Tipo Combustível (Novo): ")
                # Atualiza o modelo , cor e tipo do combustível do veiculo existente
                oracle.write(f"update veiculos set modelo_veiculo = '{novo_modelo_veiculo}', cor_veiculo = '{nova_cor_veiculo}', tipo_combustivel = '{novo_tipo_combustivel}' where codigo_veiculo = {codigo_veiculo}")
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo, modelo_veiculo, cor_veiculo, tipo_combustivel from veiculos where codigo_veiculo = {codigo_veiculo}")
                # Cria um novo objeto Veiculo
                veiculo_atualizado = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
                # Exibe os atributos do novo veiculo
                print(veiculo_atualizado.to_string())
                aux_loop = input("Deseja atualizar mais um veiculo? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O código {codigo_veiculo} não existe.")
                aux_loop = input("Deseja tentar atualizar um veiculo novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_veiculo(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":    
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código do veiculo a ser alterado
            codigo_veiculo = int(input("Código do Veiculo que irá excluir: "))        

            # Verifica se o veiculo existe na base de dados
            if not self.verifica_existencia_veiculo(oracle, codigo_veiculo):            
                # Recupera os dados do novo veiculo criado transformando em um DataFrame
                df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo, modelo_veiculo, cor_veiculo, tipo_combustivel from veiculos where codigo_veiculo = {codigo_veiculo}")
                if not self.verifica_existencia_veiculo_em_alugueis(oracle, codigo_veiculo): 
                    print("O veículo faz parte de um aluguel. Não é possível excluir!")
                    aux_loop = input("Deseja tentar excluir outro veiculo? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
                else:
                    opcao_excluir = input(f"Tem certeza que deseja excluir o veiculo de código {codigo_veiculo} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Remove o veiculo da tabela
                        oracle.write(f"delete from veiculos where codigo_veiculo = {codigo_veiculo}")            
                        # Cria um novo objeto Veiculo para informar que foi removido
                        veiculo_excluido = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
                        # Exibe os atributos do veiculo excluído
                        print("Veiculo Removido com sucesso!")
                        print(veiculo_excluido.to_string())
                        aux_loop = input("Deseja excluir mais um veiculo? (S ou N)\n").lower()
                        aux_Skip = aux_loop
                        if aux_loop == "n":
                            break;
                    if aux_Skip != "s":
                        aux_loop = input("Ainda deseja excluir um veículo? (S ou N)\n").lower()
                        if aux_loop == "n":
                            break;
            else:
                print(f"O código {codigo_veiculo} não existe.")
                aux_loop = input("Deseja tentar excluir um veiculo novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_veiculo(self, oracle:OracleQueries, codigo:int=None) -> bool:
        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo, modelo_veiculo, cor_veiculo, tipo_combustivel from veiculos where codigo_veiculo = {codigo}")
        return df_veiculo.empty
    def verifica_existencia_veiculo_em_alugueis(self, oracle:OracleQueries, codigo:int=None) -> bool:
        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = oracle.sqlToDataFrame(f"select codigo_veiculo from itens_aluguel where codigo_veiculo = {codigo}")
        return df_veiculo.empty
