from model.alugueis import Aluguel
from model.veiculos import Veiculo

class ItemAluguel:
    def __init__(self, 
                 codigo_item:int=None,
                 quantidade:float=None,
                 valor_aluguel_veiculo:float=None,
                 aluguel:Aluguel=None,
                 veiculo:Veiculo=None
                 ):
        self.set_codigo_item(codigo_item)
        self.set_quantidade(quantidade)
        self.set_valor_aluguel_veiculo(valor_aluguel_veiculo)
        self.set_aluguel(aluguel)
        self.set_veiculo(veiculo)

    def set_codigo_item(self, codigo_item:int):
        self.codigo_item = codigo_item

    def set_quantidade(self, quantidade:float):
        self.quantidade = quantidade

    def set_valor_aluguel_veiculo(self, valor_aluguel_veiculo:float):
        self.valor_aluguel_veiculo = valor_aluguel_veiculo
    
    def set_aluguel(self, aluguel:Aluguel):
        self.aluguel = aluguel

    def set_veiculo(self, veiculo:Veiculo):
        self.veiculo = veiculo

    def get_codigo_item(self) -> int:
        return self.codigo_item

    def get_quantidade(self) -> float:
        return self.quantidade

    def get_valor_aluguel_veiculo(self) -> float:
        return self.valor_aluguel_veiculo
    
    def get_aluguel(self) -> Aluguel:
        return self.aluguel

    def get_veiculo(self) -> Veiculo:
        return self.veiculo

    def to_string(self):
        return f"Item: {self.get_codigo_item()} | Quant.: {self.get_quantidade()} | Vlr. Alug.: {self.get_valor_aluguel_veiculo()} | Veic.: {self.get_veiculo().get_modelo_veiculo()} | Aluguel: {self.get_aluguel().get_codigo_aluguel()}"