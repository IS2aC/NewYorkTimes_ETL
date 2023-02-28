"""
Ce fichier python realise les operations de rapport sur les données stockées dans la database

"""
from pymongo import MongoClient
import requests
from datetime import datetime
import pandas as pd

###### DATA BASE ##################################################
client =  MongoClient("mongodb://localhost:27017/")
#nyt : new york times
database =  client['nyt']

# Récupération de tous les documents correspondants
collection = database["info_articles"]
###################################################################

#################################################################################
def dump_of_database():
    
    """ Fonction pour recuperer sous forme de dataframe l'integralité des données sur la database MONGO DB pour les operations d'analyse  """
    
    df =  pd.DataFrame()
    _id, link, published, author, category, credit, sumary, title, polarity, score = [],[], [], [],[],[],[],[],[], []

    data =  collection.find()
    for i in data:
        _id.append(i.get('_id'))
        link.append(i.get('link'))
        published.append(i.get('published'))
        author.append(i.get('author'))
        category.append(i.get('category'))
        credit.append(i.get('credit'))
        sumary.append(i.get('summary'))
        title.append(i.get('title'))
        polarity.append(i.get('polarity'))
        score.append(i.get('score'))
        

    df['_id'] =  _id
    df['link'] =  link
    df['published'] =  published
    df['author'] =  author
    df['category'] =  category
    df['credit'] =  credit
    df['sumary'] =  sumary
    df['title'] =  title
    df['polarity'] =  polarity
    df['score'] =  score

    return df
#################################################################################


######### nombre d'articles recolter depuis le debut des operations de collecte ########
def nombre_articles_total():
    
    df =  dump_of_database()
    print(f"nombre de données recoltés : {df.shape[0]}")
    print('------------------------------------------------------------')

######### afficher un rapport de classifications par categorie ciblé ########
def rapport_par_categorie():
    
    """ rapport des articles collectés par categorie depuis le debut des operations  """
    print("rapport par categorie : BUSINESS / ECONOMY ")
    compt_neg = 0
    compt_pos =  0
    category =  ['Business','Economy']
    for topic in category:
        print(topic)
        resultats = collection.find({"category":topic})
        for resultat in resultats:
            if resultat.get('polarity') == "negative":
                compt_neg += 1
            else:
                compt_pos += 1
        print(f"{topic} -- positif:{compt_pos} -- negatif:{compt_neg}")
            
    print('------------------------------------------------------------')
################################################################################





###### proportions d'articles positifs/negatifs par jour ########################
def positif_negatif_prop_jour():
    
    """ rapport de la proportion d'articles negatif ou positif par jour """
    
    # agréger les documents par jour
    pipeline = [
        {'$group': {'_id': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$published'}}, 'count': {'$sum': 1}}}
    ]

    result = collection.aggregate(pipeline)

    # afficher le résultat
    for doc in result:
        print(f"Le {doc['_id']} on a recoltés {doc['count']} articles.")
    print('------------------------------------------------------------')

#################################################################################


##### l'article avec le plus grand score negatif ##############################
def url_max_negatif():
    df =  dump_of_database()
    
    #s'assurer que la colonne est en float
    df = df.astype({'score': 'float64'})
    
    #filtrer les polarités positives
    df = df[df['polarity'] == 'negative']
    index =  df['score'].idxmax()
    
    #obtention de l'url en capturant la ligne du dataframe
    url = df.iloc[index]['link']
    
    print("la plus mauvaise annonce est a l'annonce : ", url)
    print('------------------------------------------------------------')
################################################################################


##### l'article de la journée avec le plus grand score positif ##################
def url_max_negatif():
    df =  dump_of_database()
    
    #s'assurer que la colonne est en float
    df = df.astype({'score': 'float64'})
    
    #filtrer les polarités positives
    df = df[df['polarity'] == 'positive']
    index =  df['score'].idxmax()
    
    #obtention de l'url en capturant la ligne du dataframe
    url = df.iloc[index]['link']
    
    print("la meilleure nouvelle est a l'annonce : ", url)
    print('------------------------------------------------------------')
#################################################################################




def rapport_complet():
    """ faire etalage de toutes les fonctions predefinis plus haut comme une pseudo-rapport destiné a orienté les analyses """
    nombre_articles_total()
    rapport_par_categorie()
    positif_negatif_prop_jour()
    url_max_negatif()
    url_max_negatif()
    
    
rapport_complet()