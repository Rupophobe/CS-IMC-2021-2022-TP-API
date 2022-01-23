import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    critere = req.params.get('critere')
    if not critere:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            critere = req_body.get('critere')
    
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0 or len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            if critere == 'genre' or not critere:
                cursor.execute("select t2.genre, avg(t1.runtimeMinutes) from [dbo].[tTitles] as t1 join [dbo].[tGenres] as t2 on t1.tconst = t2.tconst group by t2.genre")
                rows = cursor.fetchall()
                for row in rows:
                    dataString += f"genre={row[0]}, temps_moyen={row[1]}\n"

            elif critere == 'acteur':
                cursor.execute("select t3.primaryName, avg(t1.runtimeMinutes) from [dbo].[tTitles] as t1 join [dbo].[tPrincipals] as t2 on t1.tconst = t2.tconst join [dbo].[tNames] as t3 on t2.nconst = t3.nconst where t2.category = 'acted in' group by t3.primaryName")
                rows = cursor.fetchall()
                for row in rows:
                    dataString += f"nom={row[0]}, temps_moyen={row[1]}\n"

            elif critere == 'directeur':
                cursor.execute("select t3.primaryName, avg(t1.runtimeMinutes) from [dbo].[tTitles] as t1 join [dbo].[tPrincipals] as t2 on t1.tconst = t2.tconst join [dbo].[tNames] as t3 on t2.nconst = t3.nconst where t2.category = 'directed' group by t3.primaryName")
                rows = cursor.fetchall()
                for row in rows:
                    dataString += f"nom={row[0]}, temps_moyen={row[1]}\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"

    return func.HttpResponse(dataString)
