import csv
import unicodedata
import psycopg2

# DADOS DE CONEXÃO:
host = "localhost"
dbname = "postgres"
user = "postgres"
password = "root"
sslmode = "require"
port = "1234"

# Parte 01: Ler o arquivo csv e tratar os dados
def remover_acentos(nome):
    # Remove os acentos, mantendo apenas os caracteres ASCII
    texto_normalizado = unicodedata.normalize('NFD', nome)

    # Remove os acentos, mantendo apenas os caracteres ASCII
    texto_sem_acentos = ''.join(c for c in texto_normalizado if not unicodedata.combining(c))

    return texto_sem_acentos

def remover_mascara_matricula(matricula):
    matricula_sem_mascara = matricula.replace('.', '').replace('-', '')

    return matricula_sem_mascara
def tratamento_situacao(situacao):
    situacao = str.upper(situacao)
    is_ativo = None
    if situacao == 'ATIVO':
        is_ativo = True
    elif situacao == 'AFASTADO':
        is_ativo = False

    return is_ativo

def verificando_vinculo(vinculo_tratado):
    vinculo = None
    eh_guarda = True
    if vinculo_tratado == "COMISSIONADO" or vinculo_tratado == "TERCEIRIZADO":
        vinculo = vinculo_tratado
        eh_guarda = False
    elif vinculo_tratado == "EFETIVO NÃO GUARDA":
        vinculo = "EFETIVO"
        eh_guarda = False
    else:
        vinculo = "EFETIVO"

    return [vinculo, eh_guarda]

def tratando_viculo(vinculo):
    vinculo = str.upper(vinculo)
    str_divida = vinculo.partition("-")
    return verificando_vinculo(str_divida[2])

def fazendo_todo_tratamento(linha):
    # TRATAMENTO DE ATIVIDADE: FUNCIONANDO
    atividade = tratamento_situacao(linha[0])

    # TRATAMENTO DE VINCULO: FUNCIONANDO
    vinculo = tratando_viculo(linha[1])

    # TRATAMENTO DE NOMES: FUNCIONANDO
    nome_sem_acento = remover_acentos(linha[2])

    # TRATAMENTO DE MATRICULA:
    matricula = remover_mascara_matricula(linha[3])

    # lista_tratada = [atividade, vinculo, nome_sem_acento, matricula, is_guarda]
    return [atividade, vinculo[0], nome_sem_acento, matricula, linha[4], vinculo[1]]

def conectar():
    try:
      # conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
      conn = psycopg2.connect(database=dbname, host = host, user = user, password = password, port = port)
      cursor = conn.cursor()
      conexao = {"conexao": conn, "cursor": cursor}

      return  conexao

    except (Exception, psycopg2.Error) as error:
      print("Falha na conexão", error)

# Parte 02: Conectar com o banco e fazer os Inserts
def fazer_cadastro_servidores(item, conn, cursor):
    try:
      # Definindo a consulta SQL para inserir dados
      postgres_insert_query = """
        INSERT INTO public.tb_servidor
        (dt_criacao, genero, is_ativo, ck_matricula, nome, is_guarda, vinculo, dt_alteracao, funcao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
       """

      # Valores que serão inseridos:
      record_to_insert = ("now()", lista_servidores[4], lista_servidores[0], lista_servidores[3], lista_servidores[2], lista_servidores[5], lista_servidores[1], None, None)
      cursor.execute(postgres_insert_query, record_to_insert)

      # Confirmando a transação
      conn.commit()
      count = cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print("Falha ao inserir registro na tabela", error)
        conn.rollback()

# Parte 03: Lendo os dados do Banco
def buscando_servidores(cursor):
    cursor.execute("select * from tb_servidor te")
    results = cursor.fetchall()

    # Exibindo os dados:
    for result in results:
        print("Dados:", result)

# Fechar cursor e conexão
def desconectar(conn, cursor):
    if conn:
        cursor.close()
        conn.close()

# INICIANDO SCRIPT
conexao = conectar()

# Lendo o arquivo CSV e tratando os dados
lista_servidores = []
with open('files/lista.csv', mode='r', encoding='utf-8') as arquivo_csv:
    leitor = csv.reader(arquivo_csv, delimiter=',')
    i = 0

    # INDEX - CAMPOS: 0 - Nome Completo, 1 - Categoria, 2 - Sigla, 3 - Número de Identificação
    try:
        for linha in leitor:
            # Chamando tratamento completo
            lista_servidores.append(fazendo_todo_tratamento(linha))
            i += 1
    except IndexError:
        print("Erro na Leitura", IndexError)

# Cadastrando os Servidores
# for i in lista_servidores:
#     # Verificando se não é Guarda
#     if lista_servidores[5] == False:
#         fazer_cadastro_servidores(i, conexao["conexao"], conexao["cursor"])
#         print("\n Cadastrado")
#         print(i)


buscando_servidores(conexao["cursor"])
desconectar(conexao["conexao"], conexao["cursor"])

print("\n #### TERMINADO SCRIPT ########")
