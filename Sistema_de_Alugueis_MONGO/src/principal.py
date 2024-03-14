from utils import config
from utils.splash_screen import SplashScreen
from reports.relatorios import Relatorio
from controller.controller_veiculo import Controller_Veiculo
from controller.controller_cliente import Controller_Cliente
from controller.controller_montadora import Controller_Montadora
from controller.controller_aluguel import Controller_Aluguel
from controller.controller_item_aluguel import Controller_Item_Aluguel

tela_inicial = SplashScreen()
relatorio = Relatorio()
ctrl_veiculo = Controller_Veiculo()
ctrl_cliente = Controller_Cliente()
ctrl_montadora = Controller_Montadora()
ctrl_aluguel = Controller_Aluguel()
ctrl_item_aluguel = Controller_Item_Aluguel()

def reports(opcao_relatorio:int=0):

    if opcao_relatorio == 1:
        relatorio.get_relatorio_alugueis_por_montadora()            
    elif opcao_relatorio == 2:
        relatorio.get_relatorio_alugueis()
    elif opcao_relatorio == 3:
        relatorio.get_relatorio_veiculos()
    elif opcao_relatorio == 4:
        relatorio.get_relatorio_clientes()
    elif opcao_relatorio == 5:
        relatorio.get_relatorio_montadoras()
    elif opcao_relatorio == 6:
        relatorio.get_relatorio_itens_alugueis()

def inserir(opcao_inserir:int=0):

    if opcao_inserir == 1:                               
        novo_veiculo = ctrl_veiculo.inserir_veiculo()
    elif opcao_inserir == 2:
        novo_cliente = ctrl_cliente.inserir_cliente()
    elif opcao_inserir == 3:
        novo_montadora = ctrl_montadora.inserir_montadora()
    elif opcao_inserir == 4:
        novo_aluguel = ctrl_aluguel.inserir_aluguel()
    elif opcao_inserir == 5:
        novo_item_aluguel = ctrl_item_aluguel.inserir_item_aluguel()

def atualizar(opcao_atualizar:int=0):

    if opcao_atualizar == 1:
        relatorio.get_relatorio_veiculos()
        veiculo_atualizado = ctrl_veiculo.atualizar_veiculo()
    elif opcao_atualizar == 2:
        relatorio.get_relatorio_clientes()
        cliente_atualizado = ctrl_cliente.atualizar_cliente()
    elif opcao_atualizar == 3:
        relatorio.get_relatorio_montadoras()
        montadora_atualizado = ctrl_montadora.atualizar_montadora()
    elif opcao_atualizar == 4:
        relatorio.get_relatorio_alugueis()
        aluguel_atualizado = ctrl_aluguel.atualizar_aluguel()
    elif opcao_atualizar == 5:
        relatorio.get_relatorio_itens_alugueis()
        item_aluguel_atualizado = ctrl_item_aluguel.atualizar_item_aluguel()

def excluir(opcao_excluir:int=0):

    if opcao_excluir == 1:
        relatorio.get_relatorio_veiculos()
        ctrl_veiculo.excluir_veiculo()
    elif opcao_excluir == 2:                
        relatorio.get_relatorio_clientes()
        ctrl_cliente.excluir_cliente()
    elif opcao_excluir == 3:                
        relatorio.get_relatorio_montadoras()
        ctrl_montadora.excluir_montadora()
    elif opcao_excluir == 4:                
        relatorio.get_relatorio_alugueis()
        ctrl_aluguel.excluir_aluguel()
    elif opcao_excluir == 5:
        relatorio.get_relatorio_itens_alugueis()
        ctrl_item_aluguel.excluir_item_aluguel()

def run():
    print(tela_inicial.get_updated_screen())
    config.clear_console()

    while True:
        print(config.MENU_PRINCIPAL)
        opcao = int(input("Escolha uma opção [1-5]: "))
        config.clear_console(1)
        
        if opcao == 1: # Relatórios
            
            print(config.MENU_RELATORIOS)
            opcao_relatorio = int(input("Escolha uma opção [0-6]: "))
            config.clear_console(1)

            reports(opcao_relatorio)

            config.clear_console(1)

        elif opcao == 2: # Inserir Novos Registros
            
            print(config.MENU_ENTIDADES)
            opcao_inserir = int(input("Escolha uma opção [1-5]: "))
            config.clear_console(1)

            inserir(opcao_inserir=opcao_inserir)

            config.clear_console()
            print(tela_inicial.get_updated_screen())
            config.clear_console()

        elif opcao == 3: # Atualizar Registros

            print(config.MENU_ENTIDADES)
            opcao_atualizar = int(input("Escolha uma opção [1-5]: "))
            config.clear_console(1)

            atualizar(opcao_atualizar=opcao_atualizar)

            config.clear_console()

        elif opcao == 4:

            print(config.MENU_ENTIDADES)
            opcao_excluir = int(input("Escolha uma opção [1-5]: "))
            config.clear_console(1)

            excluir(opcao_excluir=opcao_excluir)

            config.clear_console()
            print(tela_inicial.get_updated_screen())
            config.clear_console()

        elif opcao == 5:

            print(tela_inicial.get_updated_screen())
            config.clear_console()
            print("Obrigado por utilizar o nosso sistema.")
            exit(0)

        else:
            print("Opção incorreta.")
            exit(1)

if __name__ == "__main__":
    run()