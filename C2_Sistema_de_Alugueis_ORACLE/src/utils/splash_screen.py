from conexion.oracle_queries import OracleQueries
from utils import config

class SplashScreen:

    def __init__(self):
        # Consultas de contagem de registros - inicio
        self.qry_total_veiculos = config.QUERY_COUNT.format(tabela="veiculos")
        self.qry_total_clientes = config.QUERY_COUNT.format(tabela="clientes")
        self.qry_total_montadoras = config.QUERY_COUNT.format(tabela="montadoras")
        self.qry_total_alugueis = config.QUERY_COUNT.format(tabela="alugueis")
        self.qry_total_itens_aluguel = config.QUERY_COUNT.format(tabela="itens_aluguel")
        # Consultas de contagem de registros - fim

        # Nome(s) do(s) criador(es)
        self.created_by = "Filipe Cajado e Gustavo Calanzani"
        self.professor = "Prof. M.Sc. Howard Roatti"
        self.disciplina = "Banco de Dados"
        self.semestre = "2022/2"

    def get_total_veiculos(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Retorna o total de registros computado pela query
        return oracle.sqlToDataFrame(self.qry_total_veiculos)["total_veiculos"].values[0]

    def get_total_clientes(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Retorna o total de registros computado pela query
        return oracle.sqlToDataFrame(self.qry_total_clientes)["total_clientes"].values[0]

    def get_total_montadoras(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Retorna o total de registros computado pela query
        return oracle.sqlToDataFrame(self.qry_total_montadoras)["total_montadoras"].values[0]

    def get_total_alugueis(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Retorna o total de registros computado pela query
        return oracle.sqlToDataFrame(self.qry_total_alugueis)["total_alugueis"].values[0]

    def get_total_itens_alugueis(self):
        # Cria uma nova conexão com o banco que permite alteração
        oracle = OracleQueries()
        oracle.connect()
        # Retorna o total de registros computado pela query
        return oracle.sqlToDataFrame(self.qry_total_itens_aluguel)["total_itens_aluguel"].values[0]

    def get_updated_screen(self):
        return f"""
        ########################################################
        #                   SISTEMA DE ALUGUEIS                     
        #                                                         
        #  TOTAL DE REGISTROS:                                    
        #      1 - VEICULOS:          {str(self.get_total_veiculos()).rjust(5)}
        #      2 - CLIENTES:          {str(self.get_total_clientes()).rjust(5)}
        #      3 - MONTADORAS:        {str(self.get_total_montadoras()).rjust(5)}
        #      4 - ALUGUEIS:          {str(self.get_total_alugueis()).rjust(5)}
        #      5 - ITENS DE ALUGUEIS: {str(self.get_total_itens_alugueis()).rjust(5)}
        #
        #  CRIADO POR: {self.created_by}
        #
        #  PROFESSOR:  {self.professor}
        #
        #  DISCIPLINA: {self.disciplina}
        #              {self.semestre}
        ########################################################
        """
