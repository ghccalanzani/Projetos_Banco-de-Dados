from model.montadoras import Montadora
from conexion.oracle_queries import OracleQueries

class Controller_Montadora:
    def __init__(self):
        pass
        
    def inserir_montadora(self) -> Montadora:
        aux_loop = "s"
        while aux_loop == "s":
            ''' Ref.: https://cx-oracle.readthedocs.io/en/latest/user_guide/plsql_execution.html#anonymous-pl-sql-blocks'''

            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuario o novo CNPJ
            cnpj = input("CNPJ (Novo): ")

            if self.verifica_existencia_montadora(oracle, cnpj):
                # Solicita ao usuario a nova razão social
                razao_social = input("Razão Social (Novo): ")
                # Solicita ao usuario o novo nome fantasia
                nome_fantasia = input("Nome Fantasia (Novo): ")
                # Insere e persiste a nova montadora
                oracle.write(f"insert into montadoras values ('{cnpj}', '{razao_social}', '{nome_fantasia}')")
                # Recupera os dados da nova montadora criada transformando em um DataFrame
                df_montadora = oracle.sqlToDataFrame(f"select cnpj, razao_social, nome_fantasia from montadoras where cnpj = '{cnpj}'")
                # Cria um novo objeto montadora
                novo_montadora = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                # Exibe os atributos da nova montadora
                print(novo_montadora.to_string())
                aux_loop = input("Deseja inserir mais uma montadora? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O CNPJ {cnpj} já está cadastrado.")
                aux_loop = input("Deseja tentar inserir uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def atualizar_montadora(self) -> Montadora:
        aux_loop = "s"
        while aux_loop == "s":    
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o código da montadora a ser alterado
            cnpj = int(input("CNPJ da montadora que deseja atualizar: "))

            # Verifica se a montadora existe na base de dados
            if not self.verifica_existencia_montadora(oracle, cnpj):
                # Solicita ao usuario a nova razão social
                razao_social = input("Razão Social (Novo): ")
                # Solicita ao usuario o novo nome fantasia
                nome_fantasia = input("Nome Fantasia (Novo): ")            
                # Atualiza o nome da montadora existente
                oracle.write(f"update montadoras set razao_social = '{razao_social}', nome_fantasia = '{nome_fantasia}'  where cnpj = {cnpj}")
                # Recupera os dados da nova montadora criada transformando em um DataFrame
                df_montadora = oracle.sqlToDataFrame(f"select cnpj, razao_social, nome_fantasia from montadoras where cnpj = {cnpj}")
                # Cria um novo objeto montadora
                montadora_atualizado = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                # Exibe os atributos da nova montadora
                print(montadora_atualizado.to_string())
                # Retorna o objeto montadora_atualizado para utilização posterior, caso necessário
                aux_loop = input("Deseja atualizar mais uma montadora? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;
            else:
                print(f"O CNPJ {cnpj} não existe.")
                aux_loop = input("Deseja tentar atualizar uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def excluir_montadora(self):
        aux_loop = "s"
        aux_Skip = "n"
        while aux_loop == "s":
            # Cria uma nova conexão com o banco que permite alteração
            oracle = OracleQueries(can_write=True)
            oracle.connect()

            # Solicita ao usuário o CPF da montadora a ser alterado
            cnpj = int(input("CNPJ da montadora que irá excluir: "))        

            # Verifica se a montadora existe na base de dados
            if not self.verifica_existencia_montadora(oracle, cnpj):            
                # Recupera os dados do novo montadora criado transformando em um DataFrame
                df_montadora = oracle.sqlToDataFrame(f"select cnpj, razao_social, nome_fantasia from montadoras where cnpj = {cnpj}")
                if not self.verifica_existencia_montadora_em_alugueis(oracle, cnpj):
                    print("A montadora possui alugueis pendentes. Não é possível excluir!")
                    aux_loop = input("Deseja tentar excluir outra montadora? (S ou N)\n").lower()
                    if aux_loop == "n":
                        break;
                else:
                    opcao_excluir = input(f"Tem certeza que deseja excluir a montadora de CNPJ {cnpj} [S ou N]: ")
                    if opcao_excluir.lower() == "s":
                        # Remove a montadora da tabela
                        oracle.write(f"delete from montadoras where cnpj = {cnpj}")            
                        # Cria um novo objeto montadora para informar que foi removido
                        montadora_excluido = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
                        # Exibe os atributos da montadora excluído
                        print("montadora Removida com Sucesso!")
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
                print(f"O CNPJ {cnpj} não existe.")
                aux_loop = input("Deseja tentar excluir uma montadora novamente? (S ou N)\n").lower()
                if aux_loop == "n":
                    break;

    def verifica_existencia_montadora(self, oracle:OracleQueries, cnpj:str=None) -> bool:
        # Recupera os dados do novo montadora criado transformando em um DataFrame
        df_montadora = oracle.sqlToDataFrame(f"select cnpj, razao_social, nome_fantasia from montadoras where cnpj = {cnpj}")
        return df_montadora.empty
    def verifica_existencia_montadora_em_alugueis(self, oracle:OracleQueries, cnpj:str=None) -> bool:
        # Recupera os dados do novo montadora criado transformando em um DataFrame
        df_montadora = oracle.sqlToDataFrame(f"select cnpj from alugueis where cnpj = {cnpj}")
        return df_montadora.empty
