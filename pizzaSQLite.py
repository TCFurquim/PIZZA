#Implementa os bancos de dados necessários para o programa principal 

#Documentando a abertura do banco para consultas futuras
# 1 importar o sqlite3 
# 2 -Abrir a conexao com o banco
# a variavel conn vai receber o interpetador de banco de dados criado pelo comando do sqlite3 e armazenado no arquivo rdvs.db
# Use o método connect ()
# conn = sqlite3.connect('rdvs.db')
# 3 - Definindo um cursor (Cursor � como um localizador para as inserções decomando dentro da interface do sqlite3
# Use o método cursor ()
# cursor = conn.cursor()
# 4 - Executa a consulta 
# Use o método execute ()
# cursor.execute()
# 5 - Extraia o resultado usando fetchall ()
# 6 - Fechar o cursor e os objetos de conexão.
# cursor.close()
# conn.close()
# 7 - Capture a exceção do banco de dados, se houver alguma que possa ocorrer durante o processo de conexão.
#Importando pandas para analises em conjunto com o sql
import pandas as pd
#Importa as bibliotecas para trabalhar com o sqlite3
import sqlite3
 #Cria uma funcao para povoar o banco de dados CLIENTES A PARTIR DE UM ARQUIVO .csv
#gera um objeto do tipo file
#arquivo de entrada para povoar a tabela funcionarios
arquivo = open("pedidos.csv", "r")
#faz a variável lista conter o conteudo de arquivo como lista.
lista = arquivo.readlines()

#Arquivo do Banco de Dados
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
# criando a tabela (schema)

def criaClientes():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  # conectando...
        conn = sqlite3.connect('clientes.db')
        # definindo um cursor
        cursor = conn.cursor()
        #Se já existir a tabela exclui
        cursor.execute(""" DROP TABLE IF EXISTS CLIENTES""")
        # criando a tabela CLIENTES
        cursor.execute("""
        CREATE TABLE CLIENTES (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        idade INTEGER,
        cpf     VARCHAR(11) NOT NULL,
        email TEXT NOT NULL,
        fone TEXT,
        cidade TEXT,
        uf VARCHAR(2) NOT NULL,
        criado_em DATE NOT NULL
        );
        """)

  print('Tabela CLIENTES criada com sucesso.')
  #Se existir uma tabela FUNCIONARIOS, exclui
  #o comando cursor.execute, só roda uma instrução por vez!
  fechar_conexao(conexao)
  

#Cria a tabela de CDVS
def criaCdv():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS CDVS """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS CDVS (
    num_cdv int(10) NOT NULL,
    data_print TEXT,
    matricula INT, 
    data_origem TEXT,
    hora_origem  TEXT,
    data_destino TEXT,
    hora_destino  TEXT,
    diarias_viagem FLOAT,
    diarias_hospedagem FLOAT,
    valor_adiant FLOAT,
    data_adiant TEXT,
    id_DespViajante INT NOT NULL,
    valor_reembolso FLOAT,
    despesa_viagem FLOAT,
    statusCod INT(2),  
    PRIMARY KEY (num_cdv),
    FOREIGN KEY(matricula) REFERENCES FUNCIONARIOS(matricula),
    FOREIGN KEY(statusCod) REFERENCES STATUSCOD(statusCod)
    );
  """)
  print("Tabela CDVS criada com sucesso.")
  fechar_conexao(conexao)

#Cria a tabela de STATUSCOD que irá conter os códigos aplicaveis ao CDV
def criaStatusCod():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS STATUSCOD """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS STATUSCOD (
    statusCod INT(2) NOT NULL PRIMARY KEY,
    descricao TEXT
    );
  """)
  print("Tabela STATUSCOD criada com sucesso.")
  #Coloca os valores pdrões do STATUSCOD 
  cursor.execute(""" INSERT INTO STATUSCOD VALUES  (1, 'DEVOLVIDO AO VIAJANTE'),  (2,'AGUARDANDO APROVAÇÃO NÍVEL 1' ), (3,'AGUARDANDO APROVAÇÃO NÍVEL 2' ), (4,'APROVAÇÃO FINAL NÍVEL 1' ), (5,'APROVAÇÃO FINAL NÍVEL 2'), (6,'CANCELADO'), (7,'OUTRO/DESCONHECIDO');  """)
  #Consolida no BD
  conexao.commit()
  print("Valores padrão da tabela STATUSCOD inseridos com sucesso.")
  fechar_conexao(conexao)
  
#Cria a tabela de LOG que ira conter o histórico do RDV no sistema
def criaLOG():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS LOG """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS LOG (
    id_log INT NOT NULL PRIMARY KEY,
    data_stamp TEXT,
    nCDV INT(10) NOT NULL, 
    statusCod INT,
    id_DespViajante INT NOT NULL,
    valor_reembolso FLOAT,
    conferente INT NOT NULL,
    FOREIGN KEY(nCDV) REFERENCES CDV(num_cdv),
    FOREIGN KEY(conferente) REFERENCES FUNCIONARIOS(matricula),
    FOREIGN KEY(statusCod) REFERENCES STATUSCOD(statusCod)
    );
  """)
  print("Tabela LOG criada com sucesso.")
  fechar_conexao(conexao)

#Criacao da tabela DESPVIAJANTE que irá conter os itens de despesas que ocorreram por conta do funcionario em determinada viagem
def criaDespViajante():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela DESPVIAJANTE
  cursor.execute(""" DROP TABLE IF EXISTS DESPVIAJANTE """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS DESPVIAJANTE (
    id_desp INT NOT NULL PRIMARY KEY,
    nCDV INT(10) NOT NULL, 
    id_comprovante TEXT,
    tipo TEXT,
    valor FLOAT,
    FOREIGN KEY(nCDV) REFERENCES CDV(num_cdv)
    );
  """)
  print("Tabela DESPVIAJANTE criada com sucesso.")
  fechar_conexao(conexao)


#Criacao da Tabela de DIARIAS
# Essas tabela contem todos os atributos/colunas  
#cod -  chave primaria
#tipo - tipo de diaria 
#valor - valor da diaria
def criaDiarias():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  #DESTROI A TABELA DIARIAS SE ELA JÁ EXISTIR - PARA ATULIZAR POSTERIORMENTE
  cursor.execute(""" DROP TABLE IF EXISTS DIARIAS """)
  cursor.execute("""
  CREATE TABLE DIARIAS (
  cod int(3) NOT NULL,
  regiao text NOT NULL,
  viagem int NOT NULL,
  hospedagem int NOT NULL,
  PRIMARY KEY (cod)
  );
  """)
  print("Tabela DIARIAS criada com sucesso.")
  
  #Coloca os valores pdrões de viagem 
  cursor.execute(""" INSERT INTO DIARIAS VALUES  (001, 'CAPITAL', 96, 250),  (002, 'INTERIOR', 72, 185);  """)
  #Consolid no BD
  conexao.commit()
  print("Valores padrão da tabela de DIARIAS inseridos com sucesso.")
  fechar_conexao(conexao)

#Cria a tabela de DEMANDA
#que mapeia a relação de quem e quando foi solicitada a conferência do CDV
def criaDemanda():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  cursor.execute(""" DROP TABLE IF EXISTS DEMANDA """)
  cursor.execute("""
  CREATE TABLE IF NOT EXISTS DEMANDA (
    num_id int NOT NULL,
    num_cdv int(10) NOT NULL,
    data_solicitada TEXT,
    mat_solicitante INT, 
    meio TEXT,
    PRIMARY KEY (num_id),
    FOREIGN KEY(mat_solicitante) REFERENCES FUNCIONARIOS(matricula),
    FOREIGN KEY(num_cdv) REFERENCES CDV(num_cdv)
    );
  """)
  print("Tabela DEMANDA criada com sucesso.")
  fechar_conexao(conexao)

#--------------------------
#FUNÇÕES COMPLEMENTARES DE CONSULTA E AFINS
#devolve o valor da tabela de diáriadeviagem
def voltaDiarias():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  #Verificando os dados da tabela de diarias de viagem
  cursor.execute(""" SELECT * FROM DIARIAS """)
  #cursor é um objeto, para deixa-lo legivel tem que fazer um iterador com fetchall
  for linha in cursor.fetchall():
    print(linha)
  fechar_conexao(conexao)

#Dado o tipo da Diária, devolve o valor presente na tabela
def voltaValorDiaria(tipo):
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  #Verificando os dados da tabela de diarias de viagem
  consulta = "SELECT viagem FROM DIARIAS WHERE regiao = \""+tipo+"\""
  cursor.execute(consulta)
  saida = cursor.fetchone()[0]
  fechar_conexao(conexao)
  return saida
 #return(linha)

#Dado o tipo da Diária, devolve o valor de diária de hospedagem
def voltaValorHospedagem(tipo):
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #apaga se ja existir a tabela CDV
  #Verificando os dados da tabela de diarias de viagem
  consulta = "SELECT hospedagem FROM DIARIAS WHERE regiao = \""+tipo+"\""
  cursor.execute(consulta)
  saida = cursor.fetchone()[0]
  fechar_conexao(conexao)
  return saida
 #return(linha)

#Retorna o nome de todos os funcionarios no banco
def voltaFunc():
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()
  #Verificando os dados da tabela de diarias de viagem
  cursor.execute(""" SELECT nome FROM FUNCIONARIOS """)
  #cursor é um objeto, para deixa-lo legivel tem que fazer um iterador com fetchall
  todos = cursor.fetchall()
  saida = []
  for linha in todos: 
    saida.append(linha[0])
  fechar_conexao(conexao)
  return(saida)
  


#POVOAMENTO DA TABELA FUNCIONARIOS COM UMA LISTA 
# Observações: abroe fecho o banco dentro do procedimento 
def povoa_func(lista_arquivo):
    # Iniciando a Conexão 
    conexao = iniciar_conexao()
    #definindo um cursor
    cursor = conexao.cursor()  
    #para cada linha do arquivo
    nlinha = 0
    for linha in lista_arquivo:
        #imprime o contador nlinha
        #print("linha: "+str(nlinha))      
        #quebrando a linha de acordo com o separador do arquivo 
        item = linha.split(';')
        nomeI = item[0]
        matriculaI = (item[1])
        uoI = item[2]
        nome_uoI = item[3]
        regraPHTI = item[4]
        areaPHTI = item[5]
        subareaI = item[6]
        grupoEmpI = item[7]
        subgrupoEmpI = item[8]
        telefoneI = item[9]
        #se não tiver o campo de telefone, faz aparecer NA
        if telefoneI == "": telefoneI = "NA"
        emailI = item[10]
        #trata o ultimo elemento para tirar a quebra de linha
        emailI = emailI[0:-1]
        #print(emailI)
        #print(nomeI)
        #print(item[0:11])
        #print(nomeI+','+matriculaI+','+telefoneI+','+emailI+','+uoI)
        if nlinha !=0:
            matriculaI = int(matriculaI)
            #print("Número de linha Diferente de zero")
            #Monta a consulta de inserção SQL
            ##INSERT INTO FUNCIONARIOS (nome, matricula, uo, nome_uo, regraPHT, areaPHT, subarea, grupoEmp, subgrupoEmp, telefone, email)
            cursor.execute("""
            INSERT INTO FUNCIONARIOS 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
            """ , (nomeI, matriculaI, uoI, nome_uoI, regraPHTI, areaPHTI, subareaI, grupoEmpI, subgrupoEmpI, telefoneI, emailI))
            #Grava no bd
            #print("conexao.commit()")
            conexao.commit()
        #incrementa o contador de linhas
        nlinha = nlinha + 1
    print("Dados inseridos com sucesso.")
    #Fecha o cursor
    fechar_conexao(conexao)

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
  
def voltaMatricula(nome):
  # Iniciando a Conexão 
  conexao = iniciar_conexao()
  #definindo um cursor
  cursor = conexao.cursor()  
  cons = "SELECT matricula FROM FUNCIONARIOS WHERE nome = \""+ nome + "\" "
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
  
#Insere instancias de exemplo na tabela CDVs para teste
def instancia_exemplo():
  instancia = "INSERT INTO CDVS(num_cdv, data_print, matricula, data_origem, hora_origem, data_destino, hora_destino, diarias_viagem, diarias_hospedagem,valor_adiant,data_adiant,id_DespViajante,valor_reembolso,despesa_viagem,statusCod) VALUES (3000123,'2022-07-31','22530','2022-04-01','8:00','2022-04-03','18:00',1,1,0,'2022-01-01',0,0,36.00,7);"
  consultaSQL(instancia)

#Dado os dados de uma tabela demanda, insere no BD
def InsereDemanda():
  print('ok')

#Cria todos os bancos de dados do programa
def criaBD():
  criaFunc()
  criaStatusCod()
  criaCdv()
  criaLOG()
  criaDespViajante()
  criaDiarias()
  criaDemanda()
  
#trabalhando com o Pandas integrado ao SQL 
#Por que fazer isso? Para simplificar a apresentação de resultados, já que com o pandas naõ temos
# que fazer iterador e a exibição é nativa do Streamlit

#Criando a consulta - query
#query = '''SELECT * 
#        FROM FUNCIONARIOS; '''   
# Criando um dataframe a partir de uma consulta (query) qualquer
#Originalmente segundo argumento é um objeto conexao, nesse caso é a funcao que devolve um objeto
#df = pd.read_sql_query(query,iniciar_conexao())
#print(df.head(10))




##
#Chamadas para executar na implantação.
#criaBD()
#povoa_func(lista)
#instancia_exemplo()
#voltaDiarias()
#print(voltaFunc())
#n = "THIAGO CAMPOS FURQUIM"
#s = consultaSQL("SELECT matricula FROM FUNCIONARIOS WHERE nome = \"" +n+ "\"")
#s2 = voltaMatricula(n)
#insere = "INSERT INTO FUNCIONARIOS VALUES ('zémane', 22530, '1009', 'DIG.E', 'GOGO290', 'APARECIDA DE GOIANIA', 'GO', 'A', 'B', '62999480661', 'teste@loucura.furnas.com.br')"
#try:
#      ins = consultaSQL(insere)     
 #     print("Inserção realizada com sucesso")
#except sqlite3.Error as e:
#      print("Ops... Não foi possível realizar a operação: ", e)
#return ins
#insere = "INSERT INTO FUNCIONARIOS VALUES ('zémane', 22530, '1009', 'DIG.E', 'GOGO290', 'APARECIDA DE GOIANIA', 'GO', 'A', 'B', '62999480661', 'teste@loucura.furnas.com.br')"
#ins = consultaSQL(insere)
#print (voltaValorDiaria('INTERIOR'))
#print(ins)
#imprime a consulta
#print(s, s2)

#verificaBD()
#viajantes = voltaFunc()
#print(viajantes)


