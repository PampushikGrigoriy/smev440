from neo4j import GraphDatabase
import os

class Neo4jClient:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASS", "password123")
        
        self.driver = GraphDatabase.driver(
            self.uri,
            auth = (self.user, self.password)
        )

    def close(self):
        self.driver.close()

    
    def create_fns_req_node(self, data):
        query = (
            "CREATE (f:fns_req {"
            "req_name: $name,"
            "req_file: $file,"
            "req_type: $type,"
            "state: $state,"
            "created_at: datetime()"
            "})"
        )
        
        with self.driver.session() as session:
            return session.execute_write(self._execute_create, query, data)

        def _execute_create(tx, query, params):
            result = tx.run(query, **params)
            return result.single()