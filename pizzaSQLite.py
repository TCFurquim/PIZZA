#Implementa os bancos de dados necessários para o programa principal 
#Primeira alteração - fork 1 
#Documentando a abertura do banco para consultas futuras
# 1 importar o sqlite3 
# 2 -Abrir a conexao com o banco
# a variavel conn vai receber o interpetador de banco de dados criado pelo comando do sqlite3 e armazenado no arquivo rdvs.db
# Use o método connect ()
# conexao = sqlite3.connect('pizza.db')
# 3 - Definindo um cursor (Cursor � como um localizador para as inserções decomando dentro da interface do sqlite3
# Use o método cursor ()
# cursor = conexao.cursor()
# 4 - Executa a consulta 
# Use o método execute ()
# cursor.execute()
# 5 - Extraia o resultado usando fetchall ()
# 6 - Fechar o cursor e os objetos de conexão(definimos um metodo fechar_conexao(conexao).
# cursor.close()
# conexao.close()
# 7 - Capture a exceção do banco de dados, se houver alguma que possa ocorrer durante o processo de conexão.
#Importando pandas para analises em conjunto com o sql


import pandas as pd
import numpy as np
#Importa as bibliotecas para trabalhar com o sqlite3
import sqlite3
#Arquivo do Banco de Dados, utilizado em todos sa funcoes
arquivo_db = 'pizza.db'
#Fazendo de modo elegante a abertura e fechamento de conexao
#Cria uma rotina para iniciar a conexao e trata os erros
def iniciar_conexao():
  conexao = None
  try:
      conexao = sqlite3.connect(arquivo_db)
  except sqlite3.Error as e:
      print("Ops... Deu um erro iniciando a conexao:", e)
  
  return conexao
      
def fechar_conexao(conexao):
  if conexao:
          conexao.close()


# #####CRIACAO DAS TABELA UTILIZADAS PARA O PROGRAMA
#Cria a tabela de PESSOAS
def criaPessoas():
 # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela PESSOAS
  cursor.execute(""" DROP TABLE IF EXISTS PESSOAS """)
  # criando a tabela (PESSOAS)
  cursor.execute("""
        CREATE TABLE PESSOAS (
        id_pessoa   INTEGER PRIMARY KEY AUTOINCREMENT,
        nome        TEXT,
        telefone1   TEXT,
        telefone2   TEXT,
        datacad     TEXT,
        id_endereco INTEGER,
        FOREIGN KEY ( id_endereco   )  REFERENCES ENDERECO (id_endereco) 
                );
        """)
  print('Tabela PESSOAS criada com sucesso.')

  fechar_conexao(conexao)

#CRIANDO A TABELA ENDERECO PARA SEPARAR OS ENDEREÇOS DOS DADOS DAS PESSOAS
def criaEndereco():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  cursor.execute("""
    CREATE TABLE ENDERECO (
    id_endereco INTEGER PRIMARY KEY AUTOINCREMENT,
    cep         TEXT    NOT NULL,
    logradouro  TEXT,
    numero      TEXT,
    complemento TEXT,
    referencia  TEXT,
    bairro      TEXT
    );
  """)
  print('Tabela ENDERECO criada com sucesso.')
  fechar_conexao(conexao)

# criando a tabela CLIENTES

def criaClientes():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #Se já existir a tabela exclui
  cursor.execute(""" DROP TABLE IF EXISTS CLIENTES""")
  # criando a tabela CLIENTES
  cursor.execute("""
     CREATE TABLE CLIENTES (
     id_cliente INTEGER PRIMARY KEY AUTOINCREMENT,
     id_pessoa INTEGER,
     vendedor_principal INTEGER,
     FOREIGN KEY (id_pessoa) REFERENCES PESSOA(id_pessoa), 
     FOREIGN KEY (vendedor_principal) REFERENCES VENDEDOR(id_vendedor)
     );
      """)
  print('Tabela CLIENTES criada com sucesso.')
  
  #o comando cursor.execute, só roda uma instrução por vez!
  fechar_conexao(conexao)
  
#Cria a tabela de ENTREGADORES 
def criaEntregadores():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS ENTREGADORES """)
  cursor.execute("""
  CREATE TABLE ENTREGADORES (
  id_entregador INTEGER PRIMARY KEY AUTOINCREMENT,
  id_pessoa INTEGER,
  FOREIGN KEY (id_pessoa) REFERENCES PESSOAS(id_pessoa)
  );
  """)
  print('Tabela ENTREGADORES criada com sucesso.')
  #o comando cursor.execute, só roda uma instrução por vez!
  fechar_conexao(conexao)


#Cria a tabela de VENDEDORES 
def criaVendedores():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS VENDEDORES """)
  cursor.execute("""
  CREATE TABLE VENDEDORES (
  id_vendedor INTEGER PRIMARY KEY AUTOINCREMENT,
  id_pessoa   INTEGER,
  FOREIGN KEY (id_pessoa) REFERENCES PESSOAS (id_pessoa) 
  );
  """)
  print('Tabela VENDEDORES criada com sucesso.')
  fechar_conexao(conexao)
  
#Cria a tabela de LOG que ira conter o histórico do RDV no sistema
def criaPizza():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS PIZZA """)
  #CRIANDO A TABELA PIZZA
  cursor.execute(""" 
  CREATE TABLE PIZZA (
    id_pizza INT (2)   NOT NULL,
    preco    FLOAT (2) NOT NULL,
    sabor    TXT,
    PRIMARY KEY ( id_pizza )
   );
   """)
  print('Tabela PIZZA criada com sucesso.')
  #já insere os sabores padrão o banco:
  cursor.execute(""" 
  INSERT INTO PIZZA VALUES
  (001,35,'CALABRESA'),
  (002,35,'QUEIJO'),
  (003,35,'LOMBINHO'),
  (004,35,'MISTA');
  """)
  fechar_conexao(conexao)

#CRIACAO DA TABELA DE PEDIDO (cada linha é um item de um determinado pedido)
# Essas tabela contem todos os atributos/colunas  
#id_item identificador unico de cada linha (chave primaria)
#nro_pedido - numero do pedido 
#data - data do pedido
#id_cliente - código do cliente que fez o pedido
#id_vendedor - código do vendedor que fez a venda
#id_entregador - código do entregador que irá entregar
#id_pizza	- produto/item pedido 
#qtde - quantidade do item 
#obs - observações sobre o produto/item
#endereco_entrega -  local  de entrega da pizza; chave estrangeira de um endereco cadastrado

#Criacao da tabela PEDIDOS que irá conter os itens pedidos
def criaPedidos():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela DESPVIAJANTE
  cursor.execute(""" DROP TABLE IF EXISTS PEDIDOS """)
  #CRIANDO A TABELA PEDIDOS
  cursor.execute("""
  CREATE TABLE PEDIDOS (
  id_item          INTEGER PRIMARY KEY AUTOINCREMENT,
  nro_pedido       INT,
  data             TEXT,
  id_cliente       INT,
  id_vendedor      INT,
  id_entregador    INT,
  id_pizza         INT (2),
  qtde             INT (3),
  obs              TEXT,
  endereco_entrega INT,
  FOREIGN KEY (id_cliente)
  REFERENCES CLIENTES (id_cliente),
  FOREIGN KEY (id_vendedor)
  REFERENCES VENDEDORES (id_vendedor),
  FOREIGN KEY (id_entregador)
  REFERENCES ENTREGADORES (id_entregador),
  FOREIGN KEY (id_pizza )
  REFERENCES PIZZA (id_pizza),
  FOREIGN KEY (endereco_entrega)
  REFERENCES ENDERECO (id_endereco) 
  );
  """)
  print('Tabela PEDIDOS criada com sucesso.')
  #cria um registro inicial de pedido 
  cursor.execute("""
  INSERT INTO PEDIDO VALUES (NULL,0,'2022-08-23',0,0,0,001,1,'PRIMEIRO PEDIDO  - SOMENTE TESTE', 1)
  """)
  fechar_conexao(conexao)


#Cria uma lista de pedidos de teste
#que mapeia a relação de quem e quando foi solicitada a conferência do CDV
def clientesTeste():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  cursor.execute("""
    INSERT INTO CLIENTES (?,?,?)
    VALUES (a,b,c)
    """)

  fechar_conexao(conexao)

#Funções e Procedimentos auxiliares
#Verificando todas as tabelas criadas do bd
def verificaBD():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()  
  cursor.execute(""" SELECT * FROM sqlite_master WHERE type='table' """)
  #cursor é um objeto, para deixa-lo legivel tem que fazer um iterador
  for linha in cursor.fetchall(): print(linha)
  fechar_conexao(conexao)

def consultaSQL(consulta):
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()  
  cons = consulta
  #cons = ""+ consulta +""
  cursor.execute(cons)
  #Vamos colocar o commit no caso de alteração com insert e update ja consolidar
  conexao.commit()
  saida = []
  #cursor é um objeto, para deixa-lo legivel tem que fazer um iterador
  #for linha in cursor.fetchall(): print(linha)
  
  for linha in cursor.fetchall(): 
    #jeito deselegante
    #elem = linha[0]
    #saida = saida + [elem]
    #ou com metodo
    #jeito elegante
    saida.append(linha[0])

  fechar_conexao(conexao)  
  return(saida)

#Trabalhando com data
#conversor de formato de Data BRL to US
def dataBRtoUS(dataBR):
  s = dataBR.split("/")
  #print(s)
  usdata = s[2]+'-'+s[1]+'-'+s[0]
  #print(usdata)
  return(usdata)





#--------------------------
# #############################  A FAZER PARA INCORPORAR PELO MENOS A MAIORIA DOS PEDIDOS ANTIGOS
# FUNCAO: POVOAR o BANCO DE DADOS  A PARTIR DE UM ARQUIVO CSV
#DADO UM ARQUIVO (arquivo.csv) DE TEXTO CSV COM AS PESSOAS, 
# CRIA UMA TABELA PESSOA COM TODAS AS PESSOAS, E SETA A TABELA CLIENTES COM TODAS AS PESSOAS TAMBÉM)
#cada linha do arquivo deve ser 'quebrada' em duas listas para contemplar o modelo de bd criado
#uma pessoa possui um endereço principal, que é um  apontador para um campo endereço mais detalhado.
# #Formato da linha csv:NOME ;TELEFONE1 ;TELEFONE2;CEP;ENDEREÇO;NÚMERO;COMPLEMENTO;REFERÊNCIA;BAIRRO;DATACADASTRO
#
#POVOAMENTO DA TABELAS DE PESSOAS, CLIENTES e ENDEREÇO PRINCIPAL A PARTIR DE UM ARQUIVO .csv
# Observações: abre fecho o banco dentro do procedimento 

#Abordagem 1: 
#1.1 - Transformando o csv em um dataframe pandas.
#1.2 - Operar as colunas e criar daatframes "espelhos" do modelo SQL utilizado
#1.3 - Transformar os dataframes espelhos em tabelas SQL de fato
#1.4 - Testar o BD

#caminho do arquivo de entrada para povoar a tabela pessoas
cam_csv = ('D:\\Thiago\\Meus Programas\\Python\\Projetos\\PIZZA\\planilhas_base\\Pessoas.csv')

def povoa_inicial1(cam_csv):
  #le o csv e coloca a primeira linha como cabecalho
  df = pd.read_csv(cam_csv, sep=';',encoding='ISO-8859-1',  header=0)
  #print(type(df))
     
  #manipulando  o data set para gerar datasets espelho das tabelas
  #cria uma coluna no dataset que vai ser o id_endereco de PESSOAS e PK de ENDERECO
  #inicialmente vamos atribuir 0 a todas as linhas
  df['id_pessoa'] = 0
  # Percorrendo e atualizando linhas de um dataframe.
  # Atualiza o valor da coluna id_pessoas, utilizando como valor o próprio indice da linha
  # é preciso usar o método at()
  for indice, linha in df.iterrows():
    df.at[indice , 'id_pessoa'] = linha['id_pessoa'] + indice

  #Verifica o numero de linhas da tabela
  #nlinhas = df.count()
  #print("o numero de linhas da planilha/tabela é:")
  #print(nlinhas)
  
  # Atributo columns retorna o nome das colunas do dataframe.
  #a = df.columns
  #print("Colunas: " +a)

  #Imprime as primeiras 5 linhas
  print(df.head(2))
  
  #Criando dfs clones para não alterar o df original e para excluir seletivamente 
  #para se adequar ao modelo do bd
  pessoas = df.copy()
  endereco = df.copy()
  
  #Excluir as colunas 
  # O método drop é usado para excluir dados no dataframe.
  # A opção axis=1 define que queremos excluir uma coluna e não uma linha.
  # O parâmetro inplace define que a alteração irá modificar o objeto em memória.
  
  pessoas.drop(['CEP','ENDERECO','NUMERO','COMPLEMENTO','REFERENCIA','BAIRRO'], axis=1, inplace=False)
  
  #excluindo as colunas extras de endereco...
  endereco.drop(['NOME','TELEFONE1','TELEFONE2'], axis=1, inplace=False)
  #exibindo os 5 primeiros de pessoas:
  print(pessoas.head(1))
  print(endereco.head(1))
  



  



povoa_inicial1(cam_csv)





#Abordagem 2: 
# 2.1 - Importar o arquivo csv para uma lista de linhas, abrir a conexao com o bd
# 
# 2.2 - Dividir cada linha conforme seu conteudo (PESSOA, ENDERECO) já  gerando 
#     as consultas de inserção SQL na Tabela PESSOAS e ENDERECO do modelo
# 2.3 - Incluir cada pessoa como cliente, colocar GEADEL como vendedor e cliente principal
# 2.4 - Fechar a conexao e testar o BD

arquivo = open(cam_csv, "r")

#db = nome da base de dados
#(arquivo)

def povoa_inicial2(arquivo):
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor() 
  #transforma o arquivo em uma lista de elementos, cada elemento é uma linha
  lista = arquivo.split("\n") 
  nlinha = 0
  for linha in lista:
    #imprime o contador nlinha
    #print("linha: "+str(nlinha))      
    #quebrando a linha de acordo com o separador do arquivo 
    item = linha.split(';')
    nome = item[0]
    tel1 = (item[1])
    tel2 = item[2]
    cep = item[3]
    logradouro = item[4]
    num = item[5]
    complemento = item[6]
    referencia = item[7]
    bairro = item[8]
    #normaliza a data BRL to US
    datacadastro = dataBRtoUS(item[9])
    id_end = nlinha
    #trata o ultimo elemento para tirar a quebra de linha
    #Monta a consulta de inserção SQL na tabela PESSOAS
    cursor.execute("""
      INSERT INTO PESSOAS 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
      """ , (nlinha, nome, tel1, tel2, datacadastro, id_end))
    #Grava no bd
    conexao.commit()
    
    #Monta a consulta de inserção SQL na tabela ENDERECO
    cursor.execute("""
      INSERT INTO ENDERECO 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
      """ , (id_end, cep, logradouro, num, complemento, referencia, bairro))
    #Grava no bd
    conexao.commit()
    #incrementa o contador de linhas (que tambem é o id_endereco)
    nlinha = nlinha + 1
  
  #Avisa que incluiuos dados
  print("Dados inseridos com sucesso em PESSOAS E ENDERECO.")
  #Fecha a conexao
  fechar_conexao(conexao)
  #Coloca o Geadel (posicao 1 do arquivo csv) como o vendedor principal 
  vp =  "INSERT INTO VENDEDOR VALUES(NULL,1)"
  consultaSQL(vp)
  #Coloca o Geadel como entregador principal
  ep =  "INSERT INTO ENTREGADOR VALUES(NULL,1)"
  consultaSQL(ep)
  #Insere cada pessoa colocada na tabela Pessoa (id_pessoa) na tabela CLIENTE
  #Insere uma consulta para inserir também na tabela CLIENTE
	#Pega uma lista de Ids
  ids = consultaSQL("SELECT id_pessoa FROM PESSOA")
  for n in ids:
    consulta = "INSERT INTO CLIENTE VALUES(NULL,"+ n +",1)"
    consultaSQL(consulta)  

  









   


###
#CRIACAO INICIAL DO BD E TESTES DE POVOAMETO
#Cria todos os bancos de dados do programa
def criaBD():
  criaPessoas()
  criaEndereco()
  criaClientes()
  criaVendedores()

  
  
#trabalhando com o Pandas integrado ao SQL 
#Por que fazer isso? Para simplificar a apresentação de resultados, já que com o pandas naõ temos
# que fazer iterador e a exibição é nativa do Streamlit

#Criando a consulta - query
#query = '''SELECT * 
#        FROM PESSOAS; '''   
# Criando um dataframe a partir de uma consulta (query) qualquer
#Originalmente segundo argumento é um objeto conexao, nesse caso é a funcao que devolve um objeto
#df = pd.read_sql_query(query,iniciar_conexao())
#print(df.head(10))




##
#Chamadas para executar na implantação.


