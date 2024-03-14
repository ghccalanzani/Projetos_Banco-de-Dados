from conexion.oracle_queries import OracleQueries

class Relatorio:
    def __init__(self):
        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_alugueis.sql") as f:
            self.query_relatorio_alugueis = f.read()

        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_alugueis_por_montadora.sql") as f:
            self.query_relatorio_alugueis_por_montadora = f.read()

        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_veiculos.sql") as f:
            self.query_relatorio_veiculos = f.read()

        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_clientes.sql") as f:
            self.query_relatorio_clientes = f.read()

        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_montadoras.sql") as f:
            self.query_relatorio_montadoras = f.read()

        # Abre o arquivo com a consulta e associa a um atributo da classe
        with open("sql/relatorio_itens_alugueis.sql") as f:
            self.query_relatorio_itens_alugueis = f.read()

    def get_relatorio_alugueis(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_alugueis))
        input("Pressione Enter para sair do Relatório de Alugueis")

    def get_relatorio_alugueis_por_montadora(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_alugueis_por_montadora))
        input("Pressione Enter para sair do Relatório de Montadoras")

    def get_relatorio_veiculos(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_veiculos))
        input("Pressione Enter para sair do Relatório de Veiculos")

    def get_relatorio_clientes(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_clientes))
        input("Pressione Enter para sair do Relatório de Clientes")

    def get_relatorio_montadoras(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_montadoras))
        input("Pressione Enter para sair do Relatório de Montadoras")

    def get_relatorio_itens_alugueis(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Recupera os dados transformando em um DataFrame
        print(oracle.sqlToDataFrame(self.query_relatorio_itens_alugueis))
        input("Pressione Enter para sair do Relatório de Itens de Alugueis")