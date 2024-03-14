class Veiculo:
    def __init__(self, 
                 codigo_veiculo:int=None, 
                 modelo_veiculo:str=None,
                 cor_veiculo:str=None,
                 tipo_combustivel:str=None
                ):        
        self.set_codigo_veiculo(codigo_veiculo)
        self.set_modelo_veiculo(modelo_veiculo)
        self.set_cor_veiculo(cor_veiculo)
        self.set_tipo_combustivel(tipo_combustivel)

    def set_codigo_veiculo(self, codigo_veiculo:int):
        self.codigo_veiculo = codigo_veiculo

    def set_modelo_veiculo(self, modelo_veiculo:str):
        self.modelo_veiculo = modelo_veiculo
  
    def set_cor_veiculo(self, cor_veiculo:str):
        self.cor_veiculo = cor_veiculo

    def set_tipo_combustivel(self, tipo_combustivel:str):
        self.tipo_combustivel = tipo_combustivel    

    def get_codigo_veiculo(self) -> int:
        return self.codigo_veiculo

    def get_modelo_veiculo(self) -> str:
        return self.modelo_veiculo

    def get_cor_veiculo(self) -> str:
        return self.cor_veiculo

    def get_tipo_combustivel(self) -> str:
        return self.tipo_combustivel        

    def to_string(self) -> str:
        return f"Codigo: {self.get_codigo_veiculo()} | Modelo: {self.get_modelo_veiculo()} | Cor: {self.get_cor_veiculo()} | Tipo Combustivel: {self.get_tipo_combustivel()}"