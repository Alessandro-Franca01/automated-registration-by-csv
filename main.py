import csv
import unicodedata

# Função para remover acentos e normalizar para minúsculas
def resolver_acentos(texto):
    return unicodedata.normalize('NFD', texto).encode('ascii', 'ignore').decode('utf8').casefold()

# Abrir o arquivo CSV e tratar acentuação e codificação
with open('seu_arquivo.csv', mode='r', encoding='utf-8') as arquivo_csv:
    leitor = csv.reader(arquivo_csv, delimiter=',')
    i = 0

    # INDEX - CAMPOS: 0 - Nome Completo, 1 - Categoria, 2 - Sigla, 3 - Número de Identificação
    try:
        for linha in leitor:

            nome = resolver_acentos(linha[0])
            linha[0] = nome

            # Imprimir a linha modificada
            print(str(i) + ' - ' + ', '.join(linha))
            i += 1
    except IndexError:
        print("Leitura concluida")

print('##### Terminado Script ########')
