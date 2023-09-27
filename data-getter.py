import pandas as pd
from web3 import Web3 
import web3
from eth_utils import to_checksum_address,keccak,to_hex
from web3.middleware import geth_poa_middleware
from datetime import datetime
import math
import binascii
from web3 import Web3
import hashlib
from eth_abi import abi
import json
from helper import *
import sys
import os

# Read args
args = sys.argv

if len(args) < 3:
    print("Please pass in a feed like: python data-getter.py ethereum eth-usd")
    exit()

if len(args) < 4:
    print("Please pass in a date like: python data-getter.py ethereum eth-usd 2023-01-01")
    exit()

network = args[1].lower()
feed = args[2].lower()
feed_path = network+"/mainnet/"+feed
start_date = args[3]

with open('data/feeds.json', 'r') as file:
    # load the contents of the file into a dictionary
    feeds = json.load(file)

# Check if feed exists
if feed_path not in feeds:
    print(network+"/"+feed+" Does not exist in list of Chainlink feeds")
    exit()

feed_details = feeds[feed_path]

# Read config
with open('config.json', 'r') as file:
    config = json.load(file)

# Provider URLS
provider_url = config[network]["providerUrl"]
provider_url_archive = config[network]["providerUrlArchive"]

# Connect to the Ethereum nodes
w3 = Web3(Web3.HTTPProvider(provider_url))
w3_archive = Web3(Web3.HTTPProvider(provider_url_archive))
if network != "ethereum":
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)    
    w3_archive.middleware_onion.inject(geth_poa_middleware, layer=0)    

aggregator_contract_address = feed_details["address"]

# Read ABI
aggregator_file = "abi/aggregator_abi.json" if network == "ethereum" else "abi/polygon_aggregator_abi.json"
with open(aggregator_file, 'r') as file:
    contract_abi = json.load(file)

contract, events = create_contract(w3_archive, aggregator_contract_address, contract_abi)

# get event sigs
event_sigs = calculate_event_sigs(events)
# get event params
event_params = get_event_params(events)

# Get Node operator details if file does not exist
nops_filename = "data/"+feed_details["path"]+"/nops.json"
if os.path.exists(nops_filename):
    nop_details, transmitters = read_nop_details(feed_details["path"])
else:
    print("JSON file with NOP details is missing")
    exit(0)

# Get start block
print("Getting start block...")
start_block = get_block_by_date(w3_archive, start_date)

print("Getting payments...")
payments_filename = "data/"+feed_details["path"]+"/payments.csv"
if os.path.exists(payments_filename):
    payments = pd.read_csv(payments_filename)
else:
    payments = get_payments(w3_archive, provider_url_archive, aggregator_contract_address, start_block, event_sigs, event_params, feed_details["path"], nop_details, transmitters, events, contract)


print("Getting transmissions...")
transmissions_filename = "data/"+feed_details["path"]+"/transmissions.csv"
if os.path.exists(transmissions_filename):
    transmissions = pd.read_csv(transmissions_filename)
else:
    transmissions = get_transmissions(w3_archive, provider_url_archive, aggregator_contract_address, start_block, event_sigs, event_params, feed_details["path"], nop_details, transmitters, events, contract)

print("Getting billing params...")
billing_params_filename = "data/"+feed_details["path"]+"/billing_params.json"
if os.path.exists(billing_params_filename):
    with open(billing_params_filename, 'r') as file:
        billing_params = json.load(file)
else:
    billing_params = get_billing_params(w3_archive, provider_url_archive, aggregator_contract_address, event_sigs, event_params, feed_details["path"], events, contract)

# for each transmitter
for transmitter in transmitters:
    transmitter_name = nop_details[transmitter.lower()]["name"]
    mask = transmissions.filter(like=transmitter_name+'_answer').ne(0).any(axis=1)
    op_submissions = transmissions.loc[mask]
    dir_path = "data/"+feed_details["path"]+"/per_op/"+transmitter_name
    op_payments = payments[payments["oracleName"]==transmitter_name]
    os.makedirs(dir_path, exist_ok=True)
    op_submissions.to_csv(dir_path+"/submissions.csv")
    op_payments.to_csv(dir_path+"/payments.csv")
