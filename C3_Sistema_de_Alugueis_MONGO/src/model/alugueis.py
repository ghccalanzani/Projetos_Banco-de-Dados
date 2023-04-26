from datetime import date
from model.clientes import Cliente
from model.montadoras import Montadora

class Aluguel:
    def __init__(self, 
                 codigo_aluguel:int=None,
                 data_aluguel_inicial:date=None,
                 data_aluguel_final:date=None,
                 cliente:Cliente= None,
                 montadora:Montadora=None
                 ):
        self.set_codigo_aluguel(codigo_aluguel)
        self.set_data_aluguel_inicial(data_aluguel_inicial)
        self.set_data_aluguel_final(data_aluguel_final)
        self.set_cliente(cliente)
        self.set_montadora(montadora)


    def set_codigo_aluguel(self, codigo_aluguel:int):
        self.codigo_aluguel = codigo_aluguel

    def set_data_aluguel_inicial(self, data_aluguel_inicial:date):
        self.data_aluguel_inicial = data_aluguel_inicial

    def set_data_aluguel_final(self, data_aluguel_final:date):
        self.data_aluguel_final = data_aluguel_final    

    def set_cliente(self, cliente:Cliente):
        self.cliente = cliente

    def set_montadora(self, montadora:Montadora):
        self.montadora = montadora

    def get_codigo_aluguel(self) -> int:
        return self.codigo_aluguel

    def get_data_aluguel_inicial(self) -> date:
        return self.data_aluguel_inicial

    def get_data_aluguel_final(self) -> date:
        return self.data_aluguel_final       

    def get_cliente(self) -> Cliente:
        return self.cliente

    def get_montadora(self) -> Montadora:
        return self.montadora

    def to_string(self) -> str:
        return f"Aluguel: {self.get_codigo_aluguel()} | Data_Inicial: {self.get_data_aluguel_inicial()} | Data_Final: {self.get_data_aluguel_final()} | Cliente: {self.get_cliente().get_nome()} | Montadora: {self.get_montadora().get_nome_fantasia()}"