from prefect import flow, task
from prefect.logging import get_run_logger

from pathlib import Path
from typing import List, Dict
from config import GATE_PATHS

@task(name = "Проверка входящих запросов ФНС")
def get_fns_req(db) -> List:
    logger = get_run_logger()

    prefixes = ('ZSV', 'ZSN', 'ZSO')
    logger.info(f"Поиск файлов запроса {prefixes}")

    fns_req = [f.name for f in GATE_PATHS['fns_in'].glob('*.xml') if f.name.startswith(prefixes)]
    logger.info(f"Найдено входящих запросов: {len(fns_req)}")
    
    return fns_req

@task(name = "Проверка входящих квитанций ФНС")
def get_fns_kwit() -> List:
    logger = get_run_logger()

    prefixes = ('KWIT',)
    
    fns_kwit = [f.name for f in GATE_PATHS['fns_in'].glob('*.xml') if f.name.startswith(prefixes)]
    logger.info(f"Найдено входящих квитанций: {len(fns_kwit)}")

    return fns_kwit

@task(name = "Запись в neo4j")
def write_req_to_neo4j(xml_file: str):

    name = xml_file[:-4]
    type = xml_file[:3]

    logger = get_run_logger()

    data = {
        "name": name,
        "file": xml_file,
        "type": type,
        "state": "send_to_abs"
    }

@flow(name="Проверка входящих файлов ФНС и ЕНС", log_prints=True)
def gate_in_flow():
    logger = get_run_logger()

    

    fns_req_future = get_fns_req().submit()
    fns_kwit_future = get_fns_kwit().submit()

    fns_req_list = fns_req_future.result()
    fns_kwit_list = fns_kwit_future.result()

    if fns_req_list:
        for xml_file in fns_req_list:
            write_req_to_neo4j(xml_file)



