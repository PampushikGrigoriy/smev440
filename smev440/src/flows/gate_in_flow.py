"""Gate-in flow for processing FNS and ENS incoming files."""

from pathlib import Path
from typing import List

from prefect import flow, task
from prefect.logging import get_run_logger

from src.config import GATE_PATHS
from src.neo4j_client import Neo4jClient


@task(name="Проверка входящих запросов ФНС")
def get_fns_req() -> List[str]:
    """Search for incoming FNS request files."""
    logger = get_run_logger()

    prefixes = ("ZSV", "ZSN", "ZSO")
    logger.info("Поиск файлов запроса %s", prefixes)

    fns_req = [
        f.name
        for f in GATE_PATHS["fns_in"].glob("*.xml")
        if f.name.startswith(prefixes)
    ]
    logger.info("Найдено входящих запросов: %d", len(fns_req))

    return fns_req


@task(name="Проверка входящих квитанций ФНС")
def get_fns_kwit() -> List[str]:
    """Search for incoming FNS receipt files."""
    logger = get_run_logger()

    prefixes = ("KWIT",)

    fns_kwit = [
        f.name for f in GATE_PATHS["fns_in"].glob("*.xml") if f.name.startswith(prefixes)
    ]
    logger.info("Найдено входящих квитанций: %d", len(fns_kwit))

    return fns_kwit


@task(name="Запись в neo4j")
def write_req_to_neo4j(xml_file: str) -> None:
    """Write FNS request data to Neo4j database."""
    name = xml_file[:-4]
    req_type = xml_file[:3]

    logger = get_run_logger()

    data = {
        "name": name,
        "file": xml_file,
        "type": req_type,
        "state": "send_to_abs",
    }

    logger.info("Writing request %s to Neo4j", xml_file)

    with Neo4jClient() as client:
        client.create_fns_req_node(data)

    logger.info("Request %s successfully written to Neo4j", xml_file)

@flow(name="Проверка входящих файлов ФНС и ЕНС", log_prints=True)
def gate_in_flow() -> None:
    """Main flow for checking incoming FNS and ENS files."""
    logger = get_run_logger()

    # Initialize Neo4j connection pool once per flow run
    from src.neo4j_client import get_neo4j_driver, close_neo4j_driver
    
    try:
        # Ensure driver is initialized (lazy initialization happens on first call)
        get_neo4j_driver()
        logger.info("Neo4j connection pool initialized")

        fns_req_future = get_fns_req.submit()
        fns_kwit_future = get_fns_kwit.submit()

        fns_req_list = fns_req_future.result()
        fns_kwit_list = fns_kwit_future.result()

        if fns_req_list:
            for xml_file in fns_req_list:
                write_req_to_neo4j(xml_file).result()

        logger.info("Flow completed. Processed %d FNS requests and %d receipts.",
                    len(fns_req_list), len(fns_kwit_list))
    finally:
        # Close connection pool when flow completes
        close_neo4j_driver()
        logger.info("Neo4j connection pool closed")


if __name__ == "__main__":
    gate_in_flow()