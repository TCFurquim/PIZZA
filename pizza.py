#Programa de  criação de solução de cadastro de clientes e entrega para pizzaria usando o streamlit
# instala o pacote de tratamento pdf
#!pip install pymupdf

# importa bibliotecas necessárias
import streamlit as st
import pandas as pd
import fitz
import pizzaSQLite as pdb
#Parte operacional do programa
subtexto = 'Fazer o bem pode ser mais gostoso ainda...'
sabores = ['Calabreza', 'Lombinho', 'Mista']
preco = 40
clientes = ['Joao da Silva', '38408-056','Rua A','34.9948.0661']
menuOpcao = ['Fazer Pedidos', 'Cadastrar Pizza', 'Cadastrar Cliente']


#Menu Principal
st.title("Pizza - Contole de Pedidos")
a = st.subheader(subtexto)
input_txt = st.text_input(
'NOME:',
value = 'JOAOZINHO',
max_chars = 25
)
st.write('A palavra inputada foi: ', input_txt)


#Menu Lateral - Opções Administrativas
st.sidebar.title('Menu')
#st.selectbox('Selecione uma opção', sabores)
#tipo_checagem = st.sidebar.selectbox('Como vai verificar',['Manual', 'Arquivo'])
st.sidebar.subheader ('Pedido')
if st.sidebar.button('Realizar Pedido'):
    st.write('Vamos Fazer o Seu Pedido?')
    
    

st.sidebar.subheader ('Cadastro')
if st.sidebar.button('Cliente'): 
    campos = ['NOME:', 'TELEFONE:','ENDEREÇO:']
    # i in - pega o elemento
    for i in campos:
        st.text_input(i,value = '', max_chars = 25)
        #st.write(i)
        

    
st.sidebar.button('Vendedor')
st.sidebar.button('Entregador')

#Foto Lateral
from PIL import Image
foto = Image.open('logo.png')
st.sidebar.image(foto,
         caption='Logo do Geadel',
         use_column_width=False)



#upload_file = st.sidebar.file_uploader ("Carregar um arquivo:")
#if upload_file is not None:
    #Faz a leitura do arquivo pdf e retorna o texto na variavel texto
#    with fitz.open(upload_file) as pdf:
#        texto = ""
#        for pagina in pdf:
#            texto += pagina.getText()
    #texto
    #Original transformava em um dataframe...
    #df = pd.read_csv (upload_file)
    #Mensagem de aquivo enviado com sucess
 #   st.sidebar.info('Arquivo enviado com sucesso')

#Cria um botão para o tratamento da variável
#Botão
#if st.sidebar.button('Processar RDV'):
#   st.write(texto)
#else:
   #st.write('Clique no botão lateral para Processar...')
#Faz o tratamento do arquivo pdf recebido,inicialmente só escreve o texto
#st.text(texto)

