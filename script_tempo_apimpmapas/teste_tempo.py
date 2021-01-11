import requests
import subprocess
import array
import time
import argparse
import asyncio

import aiohttp
from aiohttp import ClientSession
from tqdm import tqdm


async def call_endpoint(url, i, response_times, session):
    print(f"Calling {url}")
    t1 = time.time()
    response = await session.request(method="GET", url=url)
    t2 = time.time()

    if response.status == 200:
        print(t2-t1, url)
        response_times[i].append(t2 - t1)

async def simulate_one_client(urls, response_times):
    tasks = []
    async with ClientSession() as session:
        for i, url in enumerate(urls):
            tasks.append(call_endpoint(url, i, response_times, session))

        await asyncio.wait(tasks)

async def simulate_n_clients(response_times, endpoints, token, n=10):
    with open('ids.csv', 'r') as ids:
        lines = ids.readlines()[1:]
    
    for i in range(0, len(lines), n):
        tasks = []
        for line in lines[i:i+n]:
            orgao_id = line.split(',')[0]
            cpf = line.split(',')[1][:-1]
            
            urls = build_urls(endpoints, orgao_id, cpf, token)
            tasks.append(simulate_one_client(urls, response_times))

        await asyncio.wait(tasks)

def build_urls(endpoints, orgao, cpf, token):
    return [url.format(orgao=orgao, cpf=cpf, token=token) for url in endpoints]


parser = argparse.ArgumentParser(description="Endpoints Speed Test")
parser.add_argument('-p','--parallel', dest='runParallel', action='store_true')
parser.set_defaults(runParallel=False)
args = parser.parse_args()

RUN_PARALLEL = args.runParallel

endpoints = [
    'endpoint_dominio/saidas/{orgao}?jwt={token}',
    'endpoint_dominio/outliers/{orgao}?jwt={token}',
    'endpoint_dominio/entradas/{orgao}/{cpf}?jwt={token}',
    'endpoint_dominio/suamesa/detalhe/vistas/{orgao}/{cpf}?jwt={token}',
    'endpoint_dominio/suamesa/vistas/{orgao}/{cpf}?jwt={token}',
    'endpoint_dominio/suamesa/finalizados/{orgao}?jwt={token}',
    'endpoint_dominio/suamesa/investigacoes/{orgao}?jwt={token}',
    'endpoint_dominio/suamesa/processos/{orgao}?jwt={token}',
    'endpoint_dominio/alertas/{orgao}?jwt={token}',
    'endpoint_dominio/radar/{orgao}?jwt={token}',
    'endpoint_dominio/suamesa/lista/vistas/{orgao}/{cpf}/ate_vinte?jwt={token}',
    'endpoint_dominio/suamesa/detalhe/processos/{orgao}?jwt={token}​',
    'endpoint_dominio/suamesa/detalhe/investigacoes/{orgao}?jwt={token}',
    'endpoint_dominio/lista/processos/{orgao}?jwt={token}',
    'endpoint_dominio/tempo-tramitacao/{orgao}?jwt={token}',
    'endpoint_dominio/desarquivamentos/{orgao}?jwt={token}',
]

response_times = [[]] * len(endpoints)

token = subprocess.run(["curl -s -X POST "
"-F 'jwt=' "
"""endpoint_login | jq .token | sed -e 's/"//g'"""], shell=True, capture_output=True).stdout
token = token.decode('utf-8')[:-1]

if RUN_PARALLEL:
    asyncio.run(simulate_n_clients(response_times, endpoints, token, n=10))
else:
    with open('ids.csv', 'r') as ids:
        for line in tqdm(ids.readlines()[1:]):
            orgao_id = line.split(',')[0]
            cpf = line.split(',')[1][:-1]
            
            urls = build_urls(endpoints, orgao_id, cpf, token)
            for i, url in enumerate(urls):
                t1 = time.time()
                response = requests.get(url)
                t2 = time.time()

                if response.status_code == 200:
                    print(t2-t1, url)
                    response_times[i].append(t2 - t1)
