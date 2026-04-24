import os
from typing import Any, Dict, Optional
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import Neo4jError


class _Neo4jDriverSingleton:
    """Singleton manager for Neo4j driver to ensure single connection pool."""
    
    _instance: Optional['_Neo4jDriverSingleton'] = None
    _driver: Optional[Driver] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_driver(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> Driver:
        """Get or create the Neo4j driver instance."""
        if self._driver is None:
            self._uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
            self._user = user or os.getenv("NEO4J_USER", "neo4j")
            self._password = password or os.getenv("NEO4J_PASS", "password123")
            
            self._driver = GraphDatabase.driver(
                self._uri,
                auth=(self._user, self._password)
            )
        return self._driver

    def close(self) -> None:
        """Close the driver connection pool."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None


def get_neo4j_driver() -> Driver:
    """Get the singleton Neo4j driver instance."""
    return _Neo4jDriverSingleton().get_driver()


def close_neo4j_driver() -> None:
    """Close the singleton Neo4j driver instance."""
    _Neo4jDriverSingleton().close()


class Neo4jClient:
    """Neo4j database client using singleton driver for efficient session management."""

    def __init__(self) -> None:
        self.driver = get_neo4j_driver()

    def close(self) -> None:
        """Close the client (does not close the shared driver)."""
        pass  # Driver is managed by singleton, don't close it per client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def _execute_create(tx, query: str, params: Dict[str, Any]) -> Any:
        """Execute a write transaction."""
        result = tx.run(query, **params)
        return result.single()

    def create_fns_req_node(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create an FNS request node in the database.
        
        Args:
            data: Dictionary containing name, file, type, and state fields.
            
        Returns:
            The created node data or None if failed.
        """
        query = """
        CREATE (f:fns_req {
            req_name: $name,
            req_file: $file,
            req_type: $type,
            state: $state,
            created_at: datetime()
        })
        RETURN f
        """

        try:
            with self.driver.session() as session:
                result = session.execute_write(self._execute_create, query, data)
                return result
        except Neo4jError as e:
            raise RuntimeError(f"Failed to create FNS request node: {e}") from e