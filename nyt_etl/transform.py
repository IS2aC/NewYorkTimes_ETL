import requests
from datetime import datetime


#### source rss_xml static ####
def get_month(published):
    """" fonction pour la convertion des dates """
    dico = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
    }
    return dico[published]


def correct_date(published):
  published =  published.split()
  day, month, year = published[1], get_month(published[2]), published[3]
  hour = published[4]
  date_str = f"{day}/{month}/{year} {hour}"
  date_object = datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')

  return date_object


def transformProcess(dictionnaire):
    dictionnaire['published'] = correct_date(dictionnaire['published']) 
    return dictionnaire

###############################


###############################
def correct_date_api(date):
  """ Le requetage sur l'API requiert un format specifique qui est par exemple : 
                    20231005 ---- 2023/10/05
  La fonction donc transforme la date dans un format adapté pour le endpoint.
  
  """
  month =  str(date.month)
  day =  str(date.day)
  month = f"0{month}" if  eval(month) in range(10) else f"{month}"
  day =  f"0{day}" if  eval(day) in range(10) else f"{day}"

  return f"{date.year}{month}{day}"
###############################



############  modele Natural Language Processing ##################    
def model_sentiment_analysis(articles):
  
  """ Pour le cadre de la classiffication decide d'opter pour du transfert learning sur un model d'analyse de sentiment stocker sur huggin'Face """
  
  #end point du modele gardio entiment analyzer
  response = requests.post("https://amrrs-gradio-sentiment-analyzer.hf.space/run/predict", json={
	"data": [
		articles,
	  ]
  }).json()
  
  #recuperation du score dans la reponse json
  score =  eval(response['data'][0].split()[-1][:-2])
  
  #recuperation du label dans la reponse json
  label =  response['data'][0].split()[1].replace(',', '').replace("'","")
  
  return (label, score)
######################################################################














