import datetime

import tweepy as tt
import spacy
import re
from sql import sql_mgr as db
import time
from unidecode import unidecode

# baixando o pacote de língua portuguesa da SpaCy
nlp = spacy.load('pt_core_news_lg')

assuntos_lista = ['assexual', 'frank ocean', 'Super Mario Bros', 'Skala', 'Spotify', 'bissexual', 'Briggs', 'Ifood', 'gremio', 'bjork', 'Pelé',
                  'Drag Race', 'Corinthians', 'linguagem inclusiva', 'linguagem neutra']

vogais = ['a','e', 'i', 'o', 'u']

ofensas = ['babaca', 'arrombada', 'babacas', 'arrombadas', 'arrombados', 'arrombado']

palavroes = ['puta', 'caralho', 'buceta', 'porra', 'fudeu', 'fude', 'fuder', 'caralhos', 'bucetas', 'putas', 'xeca',
              'porras', 'poha', 'cu', 'cus', 'puteiros', 'puteiro', 'putaria', 'piroca', 'xereca', 'xana', 'xavasca', 'putinha', 'putinhas'
             , 'jeba', 'geba', 'benga', 'xaninha', 'larissinha', 'priquito', 'prikito', 'priquita', 'prikita', 'pica', 'pika', 'xibata']

abreviacoes = ['mt', 'mto', 'mta', 'mts', 'mtas', 'mtos', 'ata', 'eh', 'blza', 'ta', 'to', 'ok', 'afdm', 'sla',
                'pka', 'obv', 'ape', 'aq', 'ak', 'man', 'oxe', 'oxi', 'aff', 'affz', 'af', 'oto', 'fem', 'masc',
                'lek', 'ein', 'eim', 'aham', 'ah, ta', 'ta', 'ata', 'fi', 'fia', 'ent', 'amg', 'fav', 'bixo']

# função que extrai os substantivos de um texto
def extract_nouns(text):
    doc = nlp(text=text)

    nouns = [unidecode(token.text.strip().lower()) for token in doc if token.pos_ == 'NOUN' or token.tag_ == 'NN'
             and not token.is_stop and token.is_alpha and len(token.text) != 1 
             and token.text in palavroes and token.text not in abreviacoes and token.text not in ofensas
             and not all(v in vogais for v in token.text) and not all(v not in vogais for v in token.text)
             and len(token.lemma_) >= 3]

    return nouns   


def limpaSubstantivos(texto):
    # removendo determinados caracteres especiais, menções e links
    # usando expressões regulares

    # limpa links
    texto = re.sub(r'https\S+', '', texto)

    # limpa underlines
    texto = re.sub(r'_', '', texto)

    # limpa arrobas
    texto = re.sub(r'@', '', texto)

    # limpa pontos, vírgulas, aspas, etc
    texto = re.sub(r'[^\w\s]', '', texto)

    # limpa sequencias de caracteres incorretos, como aaaaaa ou bbbbbb
    texto = re.sub(r'^([a-z])\1+$', '', texto)
    texto = re.sub(r'^([A-Z])\1+$', '', texto)

    # limpa palavras com letras repetidas sucessivamente, por exemplo, abaaa
    texto = re.sub(r'\b\w*(\w)(\1{3,})\w*\b', '', texto)

    return texto


def autenticate():
    # suas credenciais de acesso
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_secret = ''

    # autenticação na API do twitter
    auth = tt.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    # instancia de conexão com a API
    return tt.API(auth, wait_on_rate_limit=True)


# primeira autenticação
api = autenticate()

# instancia de conexão com o banco de dados
DBconn = db.SQLEngine()

# acesso ao banco para buscar dados que já foram cadastrados, substantivos já contabilizados
updateDict = DBconn.search(None, 3) or {}

tweet_verificado = []

tweet_insert = []

insertDict = {}

tweetCountRel = {}

sqlInsertNouns = []

print(f"<Autenticado!>{ time.strftime('%H:%M:%S', time.localtime(time.time())) }")
print('--------------')

for assunto in assuntos_lista:
    print(f"<Iniciando extração de tweets> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
    print('------------------------------')
    tweets = tt.Cursor(api.search_tweets, q=assunto, lang='pt-br', count=500, tweet_mode='extended').items(2000)
    print(f"<Extração de tweets concluída> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
    print('------------------------------')

    print(f"<Iniciando limpeza, padronização e processamento dos dados> { time.strftime('%H:%M:%S', time.localtime(time.time())) } ")
    print('-----------------------------------------------------------')

    # iterando por cada um dos tweets extraídos
    for tweet in tweets:
        # se o tweet for repetido, só vá para o próximo tweet
        if tweet.full_text in tweet_verificado:
            continue

        tweet_verificado.append(tweet.full_text)
        tweet_insert.append(tweet.full_text)

        print(f"<Extraindo substantivos do tweet> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
        print("---------------------------------")
        print(f"<Removendo links e outros dados com símbolos e coisas desnecessárias> {time.strftime('%H:%M:%S', time.localtime(time.time()))}")
        print('---------------------------------------------------------------------')
        # substantivos extraídos do texto
        nouns = extract_nouns(limpaSubstantivos(unidecode(tweet.full_text.lower().strip())))
        
        lista_neutro = []

        print(f"<Iniciando contabilização dos substantivos> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
        print("-------------------------------------------")
        # verificação dos substantivos em "linguagem neutra"
        for c in nouns:
            if (c[len(c) - 2] == 'g' and c[-1] == 'o') or (c[len(c) -2] == 'g' and c[-1] == 'a'):
                lista_neutro.append(unidecode(re.sub(r".$", "ue", c).strip().lower()))
            elif c[-1] == 'o' or  c[-1] == 'a':
                lista_neutro.append(unidecode(re.sub(r'.$', 'e', c).strip().lower()))
            elif len(c) > 3 and c[-1] == 's' and c[len(c) - 3] != 'h':
                lista_neutro.append(unidecode(re.sub(r"..$", "ues", c).strip().lower()))
            elif len(c) > 3 and c[-1] == 's' and c[len(c) - 3] == 'h':
                lista_neutro.append(unidecode(re.sub(r"..$", "es", c).strip().lower()))

        for n in tweet.full_text.split():
            if unidecode(n.lower().strip()) in lista_neutro:
                if unidecode(n.lower().strip()) in list(updateDict.keys()):
                    updateDict[unidecode(n.lower().strip())] += 1
                    continue

                elif unidecode(n.lower().strip()) in list(insertDict.keys()):
                    insertDict[unidecode(n.lower().strip())] += 1
                    continue

                insertDict[unidecode(n.lower().strip())] = 1

        # realizando a contabilização de substantivos, e salvando num dicionário
        for nn in nouns:
            if nn.lower().strip() in list(insertDict.keys()):
                insertDict[unidecode(nn)] += 1
                continue

            elif nn.lower().strip() in list(updateDict.keys()):
                updateDict[unidecode(nn)] += 1
                continue

            insertDict[unidecode(nn)] = 1
        
        nouns.extend([n for n in lista_neutro if n in list(insertDict.keys())])
        nouns.extend([n for n in lista_neutro if n in list(updateDict.keys())])

        tweetCountRel[tweet.full_text] = nouns

    for tweet in tweet_insert:
        DBconn.insertTweet(db.TweetText(
            tweet
        ))

    for key in list(insertDict.keys()):
        DBconn.insertNoun(db.NounCount(
            key, insertDict[key]
        ))

    # inserindo relacionamento entre tweets e substantivos
    for tweet in tweet_verificado:
        nounsRelInsert = {chave: valor for chave, valor in updateDict.items() if chave in tweetCountRel[tweet]}
        insertToNoun = {chave: valor for chave, valor in insertDict.items() if chave in tweetCountRel[tweet]}
        insertToNoun.update(nounsRelInsert)

        for n in tweetCountRel[tweet]:
            if n in list(insertToNoun.keys()):
                DBconn.insertManyToMany(tweet, n)

    # atualizando a tabela de substantivos contabilizados
    for key in list(updateDict.keys()):
        DBconn.update(key, updateDict[key])
    
    tweet_insert.clear()
    tweet_verificado.clear()
    updateDict.update(insertDict)
    insertDict.clear()

    print(f"<Extração terminada para o assunto: {assunto}> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
    print(f'Assuntos restantes: {assuntos_lista[assuntos_lista.index(assunto) + 1:]}')
    print("----------------------------------------------")

    if not assuntos_lista[assuntos_lista.index(assunto) + 1:]:
        break

    time.sleep(900)
    
    # reautenticando
    api = autenticate()

print('---------------------')
print(f"<Raspagem finalizada> { time.strftime('%H:%M:%S', time.localtime(time.time())) }")
print('---------------------')
