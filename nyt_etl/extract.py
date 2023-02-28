"""
    Ce fichier contient toutes les operation d'extraction de notre processus ETL
    
Les sources de données principales sont:
- FLUX RSS
- API new york times

"""

from datetime import datetime

### data source 1 : flux rss ###
def extraction_rss(observation):
    
    """ Recolter les données en provenance des flux rss """
    return {
            'title' : observation['title'],
            'link' :  observation['link'],
            'summary':observation['summary'],
            'author':observation['author'],
            'published':observation['published'],
            'credit':observation.get('credit')
            }




### extraction depuis API ####
def extraction_from_api(_json):
    
    """ Recolter les données essentielles en provenance de l'API de new york times """
    
    #celui la --- a.json().get('response').get('docs')[i]
    return {
        'title': _json.get('headline').get('main') ,
        'link': _json.get('web_url'),
        'summary':_json.get('abstract'),
        'author':_json.get('byline').get('original'),
        'published': datetime.strptime(_json.get('pub_date'), '%Y-%m-%dT%H:%M:%S%z'),
        'credit' :_json.get('source')
        } 

#################################







