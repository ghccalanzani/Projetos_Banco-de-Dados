# C3_BancoDeDados
 Trabalho C3 de Banco de Dados - Sistema de Aluguel de Veículos
 
Vídeo demonstrativo das funções do projeto:

Link: https://youtu.be/e0WVp3DpTc8


O sistema é uma continuação do trabalho C2, portanto exige que as tabelas existam no banco Oracle. 
Então basta executar o script Python a seguir para criação das tabelas e preenchimento de dados de exemplos:
```shell
~$ python create_tables_and_records.py
```

Depois disso, basta executar o script Python a seguir para criação das coleções e preenchimento de dados de exemplos no MongoDB:
```shell
~$ python createCollectionsAndData.py
```

Para executar o sistema basta executar o script Python a seguir:
```shell
~$ python principal.py
```

### Bibliotecas Utilizadas
- [requirements.txt](src/requirements.txt): `pip install -r requirements.txt`

