"""
    Ce fichier presente l'operation de loading assoicé a notre prcessus ETL
    
Les données pour le cas d'usage sont stockées dans une Base de données orienté
"""



def loading_articles(obs_transform, db, col):
    
    """ Fonction de loading dans la database en stipulant le nom de la base de données et de la collection
    
    """
    collection = db[col]
    #realisation du upsert sur le lien de l'article qui est naturellement unique
    load  = collection.update_one({"link":obs_transform['link']},{"$set":obs_transform}, upsert = True)



