import pdb
import json
import argparse 
import logging

from datetime import datetime
from hexbytes import HexBytes
from pathlib import Path

from web3 import Web3
from web3.providers.rpc import HTTPProvider
from eth_abi.codec import ABICodec
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data
from attributedict.collections import AttributeDict

import pandas as pd 
import numpy as np


ENV_PATH = Path.cwd()

def set_logger(filename):
    logger = logging.getLogger("on_chain_data_collector")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    filepath = ENV_PATH.joinpath('logs')
    Path(filepath).mkdir(exist_ok=True)
    file_handler = logging.FileHandler(filepath.joinpath(filename))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

parser = argparse.ArgumentParser(description='Input setting filename.')
parser.add_argument('--setting_filename', type=str,
                    help='.json file for data setting')
input_args = parser.parse_args()
setting_filename = input_args.setting_filename

logger = set_logger(f"On chain data analysis - {datetime.now().date()}.log")
logger.info(f'Input file: {Path(setting_filename)}')

if not setting_filename.endswith('.json'):
    logger.error(f'Not json type: input file {Path(setting_filename)}')
    raise Exception("Not a json file!")
    
path_setting = Path(setting_filename)
if not path_setting.exists():
    logger.error(f'Input file {Path(setting_filename)} does not exist.')
    raise Exception("Setting file does not exist!")

with open(path_setting, "r") as json_f:
    setting_parameters = json.load(json_f)

if "abi_file" in setting_parameters:
    with open(setting_parameters["abi_file"], "r") as json_f:
        abi = json.load(json_f)
else:
    logging.info(f"ABI file not found in setting file, using ERC20.json by default")
    with open(ENV_PATH.joinpath('abis').joinpath('erc20_abi.json'), "r") as json_f:
        abi = json.load(json_f)

if "to_block" in setting_parameters:
    to_block = setting_parameters["to_block"]
else:
    logging.info(f"Parameter 'to_block' not found in setting file, using 'latest' by default")
    to_block = 'latest'
    
if "from_block" in setting_parameters:
    from_block = setting_parameters["from_block"]
else:
    logging.info(f"Parameter 'from_block' not found in setting file, using same block as 'to_block' by default")
    from_block = to_block

if "node_url" in setting_parameters:
    node_url = setting_parameters["node_url"]
else:
    logging.info(f"Parameter 'node_url' not found in setting file")
    
if "contract_address" in setting_parameters:
    contract_address = setting_parameters["contract_address"]
else:
    logger.error('Contract address not found.')
    raise Exception("Contract address not found in setting file.")
    
if "event_type" in setting_parameters:
    event_type = setting_parameters["event_type"]
else:
    logger.error('Event type not found.')
    raise Exception("Event type not found in setting file.")

if "bool_tx_receipt" in setting_parameters:
    bool_tx_receipt = setting_parameters["bool_tx_receipt"]
else:
    bool_tx_receipt = False

if not bool_tx_receipt:
    logger.info('Not retrieving transaction receipts.')
    
if "bool_tx" in setting_parameters:
    bool_tx = setting_parameters["bool_tx"]
else:
    bool_tx = False

if not bool_tx:
    logger.info('Not retrieving transaction.')

if "output_csv_filename" in setting_parameters:
    output_csv_filename = setting_parameters["output_csv_filename"]
else:
    ENV_PATH.joinpath(setting_parameters["project"]).mkdir(exist_ok=True)
    output_csv_filename = ENV_PATH.joinpath(setting_parameters["project"]).joinpath('data.csv')
    logger.info(f'Output csv filename not found: mkdir({ENV_PATH.joinpath(setting_parameters["project"])}).')
    logger.info(f'Output csv filename not found: use ({output_csv_filename}).')

provider = HTTPProvider(node_url)
web3 = Web3(provider)

# event_signature_hash = web3.keccak(text=event_signature_text).hex()
contract_address = Web3.to_checksum_address(contract_address)
contract = web3.eth.contract(address=contract_address, abi=abi)
# event_filter = web3.eth.filter({
#     "address": contract_address,
#     "topics": [event_signature_hash,],
#     })
if to_block == 'latest':
    to_block = web3.eth.get_block_number()

chunk_size = 10_000
events_list = []
block_cursor = from_block
while(block_cursor < to_block):
    print(f"current block number: {block_cursor}")
    print(f"process: {np.round((block_cursor - from_block)/(to_block - from_block), 4)*100}%")
    try:
        chunk_start, chunk_end = block_cursor, min(block_cursor + chunk_size - 1, to_block)
        event_string = f"contract.events.{event_type}.get_logs(fromBlock=chunk_start, toBlock=chunk_end)"
        event_logs = eval(event_string)
        events_list.append(event_logs)
        block_cursor += chunk_size
        chunk_size = 10_000
        
    except ValueError:
        chunk_size = max(int(chunk_size/10), 1)

events = []
for sublist in events_list:
    for event in sublist:
        events.append(event)

rows = []
for idx_event, event in enumerate(events):
    if idx_event % 1000 == 1:
        print(f"Current batch: {idx_event}/{len(events)} = {idx_event/len(events)*100}%")
        df = pd.DataFrame(rows)
        Path(output_csv_filename).parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(output_csv_filename)
        
    row = dict(event)
    row = {key: val.hex() if isinstance(val, HexBytes) else val for key, val in row.items()}
    if bool_tx_receipt:
        tx_receipt = web3.eth.get_transaction_receipt(event['transactionHash'])
        tx_receipt = {key: val.hex() if isinstance(val, HexBytes) else val for key, val in tx_receipt.items()}
        row.update(tx_receipt)
    if bool_tx:
        tx = web3.eth.get_transaction(event['transactionHash'])
        tx = {key: val.hex() if isinstance(val, HexBytes) else val for key, val in tx.items()}
        row.update(tx)
    rows.append(row)
    
    # if idx_event % 10000 == 0:
    #     df = pd.DataFrame(rows)
    #     Path(output_csv_filename).parent.mkdir(exist_ok=True)
    #     df.to_csv(output_csv_filename)

df = pd.DataFrame(rows)
Path(output_csv_filename).parent.mkdir(exist_ok=True)
df.to_csv(output_csv_filename)

pdb.set_trace()