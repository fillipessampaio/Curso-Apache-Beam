import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
import re

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)


def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas
    Retorna um dicionario
    """
    return dict(zip(colunas, elemento))


def trata_datas(elemento):
    """
    Recebe um dicionario e cria um novo campo com ANO-MES
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento


def chave_uf(elemento):
    """
    Recebe um dicionario
    Retorna uma tupla com o estado(UF) e o elemento(dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)


def casos_dengue(elemento):
    """
    Recebe uma tupla('UF', [{}, {}])
    Retorna uma tupla ('UF-ANO-MES', 'casos')
    """
    uf, registros = elemento
    for registro in registros:
        if registro['casos'] == None or registro['casos'] == '':
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
        else:
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))


def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla contendo chave('UF-ANO-MES') e o volume de chuva
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return (chave, mm)


def arrendonda(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla com valor arrendondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))


def filtra_campos_vazios(elemento):
    """
    Recebe elementos
    Retorna elementos excluindo os que possuam campos vazios
    """
    chave, dados = elemento
    if all([dados['chuvas'], dados['dengue']]):
        return True
    else:
        return False


def descompactar_elementos(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla com dados descompactados
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)


def preparar_csv(elemento, delimitador=';'):
    """
    Recebe um tupla
    Retorna uma string delimitada
    """
    return f"{delimitador}".join(elemento)


dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar cases de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #| "Mostrar resultados" >> beam.Map(print)
)


chuvas = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuva)" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma da quantidade de chuva pela chave" >> beam.CombinePerKey(sum)
    | "Arrendondar resultado de chuvas" >> beam.Map(arrendonda)
    #| "Mostrar resultados (chuva)" >> beam.Map(print)
)


resultado = (
    #"""Metodo 1"""
    #(chuvas, dengue)
    #| "Empilha as pcollections" >> beam.Fratten()
    # "Agrupa as pcollections" >> beam.GroupByKey()
    #| "Mostrar resultado da uniao" >> beam.Map(print)
    
    #"""Metodo 2"""
    ({'chuvas': chuvas, 'dengue': dengue})
    | "Mesclar pcollections" >> beam.CoGroupByKey()
    | "Filtrar dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar csv" >> beam.Map(preparar_csv)
    #| "Mostrar resultado da uniao" >> beam.Map(print)

)

header = 'UF;ANO;MES;CHUVA;DENGUE'
resultado | "Criar arquivo csv" >> WriteToText('resultado', file_name_suffix='.csv', header=header)


pipeline.run()