import requests
import traceback
import json
import time
from config.config import *
import libs.database.db_sql as db
from unicodedata import normalize


def remover_acentos(txt):
    return normalize('NFKD', txt).encode('ASCII', 'ignore').decode('ASCII')


def converter_tempo(tempo):
    tempo_conv = '{0:02.0f}:{1:02.0f}'.format(*divmod(tempo, 60))
    return tempo_conv


def busca_token():
    try:

        body = {
            'email': WS['email'],
            'senha': WS['senha']
        }
        inicio = time.time()

        requisicao_token = requests.post(WS_GAZIN['url_token'], json=body)

        tempo = time.time() - inicio
        tempo_conv = converter_tempo(tempo)

        retorno = json.loads(requisicao_token.text.replace("'", '\\"'))
        print('Resposta Autenticacao-Token: ', remover_acentos(str(retorno)) + ' - tempo: ' + tempo_conv)

        sucesso = True if requisicao_token.status_code in (200, 201) else False

        sql = ' '

        db.insert(sql, params=(
            remover_acentos(retorno.get('mensagem')), remover_acentos(retorno.get('resposta').get('token')), sucesso,
            '3', tempo_conv), database='database')

        return retorno.get('resposta').get('token')

    except Exception as e:
        erro = f'Erro ao busca token  {traceback.format_exc()}'
        print(erro)


def envia_ocorrencia(registro, token):
    try:
        body = [{
            'embarque': {
                'numero': registro['numero'],
                'serie': registro['serie']
            },
            'embarcador': {
                'cnpj': registro['embarcador']
            },
            'ocorrencia': {
                'tipoEntrega': registro['ocorrencia'],
                'dtOcorrencia': registro['data_ocorrencia']
            }
        }]
        inicio = time.time()

        requisicao_ocorrencia = requests.post(WS['url_ocorren'], json=body, headers={'Authorization': token})

        tempo = time.time() - inicio
        tempo_conv = converter_tempo(tempo)

        if requisicao_ocorrencia.status_code == 401 or requisicao_ocorrencia.status_code == 200:
            retorno = requisicao_ocorrencia.text.replace("'", '\\"')

            mensagem = retorno
            protocolo = 'Sem protocolo'
        else:
            retorno = json.loads(requisicao_ocorrencia.text.replace("'", '\\"'))

            mensagem = (remover_acentos(retorno.get('mensagem')) + remover_acentos(
                str(retorno.get('erros')).replace('[', '').replace(']', '')))
            protocolo = retorno.get('protocolo')

        print('Resposta EmbarqueGazin: ', remover_acentos(str(retorno)) + ' - tempo: ' + tempo_conv)

        if ((requisicao_ocorrencia.status_code == 200) or (requisicao_ocorrencia.status_code == 201)):
            sucesso = True
        else:
            sucesso = False

        sql = ' '

        db.insert(sql, params=(
            mensagem, protocolo, sucesso, registro['registro'], '3', tempo_conv),
                  database='database')

        return requisicao_ocorrencia.status_code
    except Exception as e:
        erro = f'Erro ao enviar ocorrecias  {traceback.format_exc()}'
        print(erro)


def busca_ocorrencias():
    sql = ("""
            
          """)

    data = db.select_dict(sql, database='database')

    token = busca_token()

    for registro in data:
        retorno = envia_ocorrencia(registro, token)
        if retorno == 401:
            token = busca_token()
            retorno = envia_ocorrencia(registro, token)


if __name__ == '__main__':
    busca_ocorrencias()
