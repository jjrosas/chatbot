import pandas as pd
from py2neo import Graph

def get_data_neo(query, credentials):
    """
    Execute a query in a neo4j database and return a dataframe

    query: str
        cypher query
    credentials: dict
        dict of neo4j credentials
    """
    graph = Graph(credentials.get("URL"))
    return graph.run(query).to_data_frame()
