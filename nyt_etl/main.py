import pandas as pd
from pymongo import MongoClient
import feedparser as fd
import json, json, schedule, time
import pandas as pd
from extract import extraction_rss, extraction_from_api
from transform import transformProcess, correct_date_api, model_sentiment_analysis
from load import loading_articles
from rapport import rapport_complet
from datetime import datetime
import requests, os
from dotenv import load_dotenv
load_dotenv()

######### get date of the day ########
date_of_the_day =  correct_date_api(datetime.now())
######################################


###### DATA BASE ##################################################
client =  MongoClient("mongodb://localhost:27017/")
#nyt : new york times
database =  client['nyt']
###################################################################


###### data sources -- flux rss ###################################
business_rss = fd.parse("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml")
economy_rss = fd.parse('https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml')

nombre_articles_business = len(business_rss.get('entries')) 
nombre_articles_econo =  len(economy_rss.get('entries')) 
###################################################################


##### source API nyt ##############################################
API_KEY = os.getenv("SECRET_KEY")
topics =  ['Economy','Business']

def generator_url(topic, page, begin_date):
  url = f"https://api.nytimes.com/svc/search/v2/articlesearch.json?page={page}&begin_date={begin_date}&q={topic}&api-key={API_KEY}"
  return url
###################################################################



######### pipeline EXTRACTION , TRANSFORMATION ############################
def pipeline_extract_transform(observation):
    
    """  Fonction realisant les opeartions d'extraction  des données collectés des flux  rss et de transformation de données """
    
    observation = dict(observation)
    #recuperation de l'article
    obs = extraction_rss(observation)
    
    #operation de transformation 
    obs_transform = transformProcess(obs)
    
    return obs_transform
###########################################################################
    


##### processus executionelle data source 1 : FLUX RSS ####################
#### cas bus_articles:#####################################################
def process_etl_rss():
    
    """ Depile le processus etl jusqu'au stockage pour les données en provenace des flux rss de new yok times
                                en specifiant les category business et economy.
    """
    
    #### collection des articles de la categorie business ######################
    for i in range(nombre_articles_business):
        data =  business_rss.get('entries')[i]
        
        #processus d'extraction et de transformation
        data_transform = pipeline_extract_transform(data)
        
        #creation de la colonnes category
        data_transform['category'] =  "Business"
        
        #creation de la colonne polarité -- generé exclusivement par le modele de predictif
        data_transform['polarity'] = model_sentiment_analysis(data_transform.get('summary'))[0].lower()
        
        #creation de la colonne score
        data_transform['score'] = model_sentiment_analysis(data_transform.get('summary'))[1]
        
        #stockage dans la database
        load = loading_articles(data_transform, db = database, col = "info_articles")
        
    #### collection des articles de la categorie economie #######################
    for i in range(nombre_articles_econo):
        data =  economy_rss.get('entries')[i]
        
        #extraction + transformation des données obtenus
        data_transform = pipeline_extract_transform(data)
        
        #creation de la colonnes category
        data_transform['category'] =  "Economy"
       
        #creation de la colonne polarité -- generé  par le modele de predictif
        data_transform['polarity'] = model_sentiment_analysis(data_transform.get('summary'))[0].lower()
        
        #creation de la colonne score
        data_transform['score'] = model_sentiment_analysis(data_transform.get('summary'))[1]
        
        
        #stockage dans la database
        load = loading_articles(data_transform,  db = database, col = "info_articles" )
        
        
##################################################################################
#### processus executionelle data sourCe 2 : API articles search #################
def process_etl_search_api_new_york_times():
    
    """ Depile le processus etl jusqu'au stockage pour les données en provenace de l'api  articles search de new yok times
                                en specifiant les category business et economy.
    """
    
    for topic in topics:
        # pour chaque iteration 
        for j in range(11):
            response =  requests.get(generator_url(topic, j, date_of_the_day))
            if response.status_code == 200:
                data =  response.json()['response']['docs']
                for ii in range(len(data)):
                    data[ii] = extraction_from_api(data[ii])
                    load =  loading_articles(data[ii], db = database, col="info_articles")
            else:
                # print("Connexion non etatlit ! ")
                pass


################################################################################
##### automatisation des workflows avec le module schedule #####################
schedule.every(30).seconds.do(process_etl_rss)
schedule.every(30).seconds.do(process_etl_search_api_new_york_times)
schedule.every(30).seconds.do(rapport_complet)

compteur = 0
# # # # Boucle d'exécution  ######################################################
while True:
    schedule.run_pending()
    time.sleep(1)
    compteur += 1
    print(compteur)
################################################################################
