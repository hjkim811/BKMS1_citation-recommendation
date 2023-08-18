import pandas as pd
import ast
from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    
def graph_recommendation(title_list, k=5):
    dbname = "teamdb16"
    uri_param = "bolt://147.47.200.145:37687"
    user_param = "team16"
    pwd_param = "bdai2023"
    conn = Neo4jConnection(uri=uri_param, user=user_param, pwd=pwd_param)
    
    score_dict = {}
    for title in title_list:
        # subgraph 초기화
        cypher = '''CALL gds.graph.drop('citations', false) YIELD graphName'''
        conn.query(cypher, db=dbname)
        
        # edge 3개 이내 그래프 저장
        cypher = f'''CALL gds.graph.project.cypher("citations","MATCH (:Paper {{title:'{title}'}})-[rel:REF*1..2]-(p:Paper) RETURN id(p) as id","MATCH (p1:Paper {{title:'{title}'}})-[rel:REF*1..2]-(p2:Paper) RETURN id(p1) AS source, id(p2) AS target") YIELD graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels;'''
        conn.query(cypher, db=dbname)
        
        # article rank 계산
        cypher = '''CALL gds.articleRank.stream('citations') YIELD nodeId, score RETURN gds.util.asNode(nodeId).title AS title, score ORDER BY score DESC, title ASC'''
        response = conn.query(cypher, db=dbname)
        
        if not response:
            continue
            
        for i in response:
            score_dict[i['title']] = score_dict.get(i['title'], 0) + i['score']
            
    conn.close()     
    df = pd.DataFrame(score_dict.items(), columns=['title','score'])
    df = df[~df['title'].isin(title_list)]
    df = df.sort_values('score', ascending=False, ignore_index=True)
    return df.head(k)