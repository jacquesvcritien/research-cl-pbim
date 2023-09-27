import pandas as pd
from web3 import Web3 
import web3
import eth_utils
from eth_utils import to_checksum_address,keccak,to_hex
from datetime import datetime
import math
import binascii
from web3 import Web3
import hashlib
from eth_abi import abi
import requests
import json
from bs4 import BeautifulSoup
import re
import os
from web3.logs import STRICT, IGNORE, DISCARD, WARN

def create_contract(w3, aggregator_contract_address, contract_abi):
    """
    Creates a contract instance from the abi and address

    Args:
        w3: web3 Instance
        aggregator_contract_address: Contract's address 
        contract_abi: Contract's abi.

    Returns:
        Contract instance, Contract's ABI Events
    """
    contract = w3.eth.contract(address=aggregator_contract_address, abi=contract_abi)
    abi_events = [abi for abi in contract.abi if abi["type"] == "event"]

    return contract, abi_events

def calculate_event_sigs(abi_events):
    """
    Get Event signatures from ABI's events

    Args:
        abi_events: Contract's ABI Events

    Returns:
        Contract's event signatures
    """
    event_sigs = {}
    for event in abi_events:
        # Define the event name and parameter types
        event_name = event["name"]
        parameter_types = []
        for inp in event["inputs"]:
            parameter_types.append(inp["type"])

        event_signature = f"{event_name}({','.join(parameter_types)})"
        event_hash = Web3.keccak(text=event_signature).hex()
        
        event_sigs[event_name] = event_hash
    return event_sigs

def get_event_params(abi_events):
    """
    Get Event parameters from ABI's events

    Args:
        abi_events: Contract's ABI Events

    Returns:
        Contract's event parameters
    """
    event_params = {}
    for event in abi_events:
        # Define the event name and parameter types
        event_name = event["name"]
        parameter_types = []
        for inp in event["inputs"]:
            parameter_types.append(inp["type"])
        
        event_params[event_name] = {"params": parameter_types, "name": event_name}
    return event_params

def get_block_number_by_timestamp(w3, target_timestamp):
    """
    Get Block number from timestamp

    Args:
        w3: web3 Instance
        target_timestamp: Timestamp of the block

    Returns:
        The block number at the timestamp
    """
    latest_block_number = w3.eth.get_block('latest')['number']
    lower_bound = 0
    upper_bound = latest_block_number

    while lower_bound <= upper_bound:
        mid_block_number = math.floor((lower_bound + upper_bound) / 2)
        mid_block = w3.eth.get_block(mid_block_number)

        if mid_block.timestamp <= target_timestamp:
            next_block = w3.eth.get_block(mid_block_number + 1)
            if next_block.timestamp > target_timestamp:
                return mid_block_number
            lower_bound = mid_block_number + 1
        else:
            upper_bound = mid_block_number - 1

def get_block_by_date(w3, date):
    """
    Get Block number from date

    Args:
        w3: web3 Instance
        date: Date of the block

    Returns:
        The block number at the date
    """
    date_string = date
    date_format = "%Y-%m-%d"

    # Convert date string to a datetime object
    date_obj = datetime.strptime(date_string, date_format)

    # Convert datetime object to Unix timestamp (seconds since epoch)
    timestamp = int(date_obj.timestamp())

    return get_block_number_by_timestamp(w3, timestamp)

# Get nop details
def get_payee_addresses_changes(provider_url, aggregator_contract_address, event_sig):
    """
    Get Events of when the payee addresses changed

    Args:
        provider_url: Endpoint of the node to query
        aggregator_contract_address: The address of the contract to query
        event_sig: The event signatures

    Returns:
        An array of events for payee address changes
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [
            {"fromBlock": "0x0", 
             "address": aggregator_contract_address, 
             "topics": [event_sig]
            }
        ],
        "id": 1,
    }
    
    response = requests.post(provider_url, json=payload)
    events = json.loads(response.text)["result"]
    return events

def get_oracle_index_from_cl(transmitter, oracles):
    """
    Get an operator's index from Chainlink's contract

    Args:
        transmitter: The address of the operator to obtain
        oracles: List of operators

    Returns:
        The index of the operator from CL. If not found, -1 is returned
    """
    for index, oracle in enumerate(oracles):
        if oracle["nodeAddress"].lower() == transmitter.lower():
            return index
    
    return -1

def read_nop_details(feed_path):
    """
    Read node operators' details from file

    Args:
        feed_path: The path of the feed to query

    Returns:
        Dict of node operators' details from file
    """
    nops_filename = 'data/'+feed_path+'/nops.json'
    with open(nops_filename, 'r') as f:
        nop_data = json.load(f)
        return nop_data["nops_details"], nop_data["transmitters"]

def get_decoded_logs(abi_events, receipt, contract):
    """
    Decode logs from receipt

    Args:
        abi_events: The events from a contract's ABI
        receipt: The transaction receipt
        contract: The contract instance

    Returns:
        An array of decoded logs from the receipt
    """
    logs = []
    for log in receipt["logs"]:
        receipt_event_signature_hex = log["topics"][0].hex()
        
        for event in abi_events:
            # Get event signature components
            name = event["name"]
            inputs = [param["type"] for param in event["inputs"]]
            inputs = ",".join(inputs)
            # Hash event signature
            event_signature_text = f"{name}({inputs})"
            event_signature_hex = to_hex(keccak(text=event_signature_text))
            # Find match between log's event signature and ABI's event signature
            if event_signature_hex == receipt_event_signature_hex:
                # Decode matching log
                decoded_logs = contract.events[event["name"]]().process_receipt(receipt, errors=DISCARD)
                logs.append({"event": event["name"], "data": decoded_logs})
    
    return logs

def get_logs(provider_url, aggregator_contract_address, topic, fromBlock):
    """
    Function to query logs from a node

    Args:
        provider_url: The endpoint of the node to query
        aggregator_contract_address: The aggregator's contract address to query
        topic: The topic to get logs for
        fromBlock: The minimum block number from which to get blocks

    Returns:
        An array of events for the given topic
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [
            {"fromBlock": fromBlock, 
             "address": aggregator_contract_address, 
             "topics": [topic]
            }
        ],
        "id": 1,
    }
    
    response = requests.post(provider_url, json=payload)
    events = json.loads(response.text)["result"]
    return events

def get_logs_throttled(provider_url, aggregator_contract_address, topic, fromBlock, toBlock):
    """
    Function to query logs from a node in a throttled way

    Args:
        provider_url: The endpoint of the node to query
        aggregator_contract_address: The aggregator's contract address to query
        topic: The topic to get logs for
        fromBlock: The minimum block number from which to get blocks
        toBlock: The maximum block number from which to get blocks

    Returns:
        An array of events for the given topic
    """

    # Throttle amount
    skip = 100000
    number_of_loops = ((toBlock - fromBlock) // skip) + 1
    counter = 1
    all_events = []
    for i in range(fromBlock, toBlock + 1, skip):
        print("Making request "+str(counter)+"/"+str(number_of_loops))
        
        if i + skip < toBlock:
            print(i, i+skip)
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {"fromBlock": hex(i),
                    "toBlock": hex(i+skip),
                    "address": aggregator_contract_address, 
                    "topics": [topic]
                    }
                ],
                "id": 1,
            }
        else: 
            print(i, toBlock)
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {"fromBlock": hex(i),
                    "toBlock": hex(toBlock),
                    "address": aggregator_contract_address, 
                    "topics": [topic]
                    }
                ],
                "id": 1,
            }

        response = requests.post(provider_url, json=payload)
        events = json.loads(response.text)["result"]
        if len(events) > 0:
            all_events.extend(events)
        counter += 1
    return all_events

def get_transaction_details(w3, abi_events, tx_hash, decode_logs, contract):
    """
    Function to get a specific transaction's details

    Args:
        w3: web3 Instance
        abi_events: The events from a contract's ABI
        tx_hash: The hash of the transaction for which to get details
        decode_logs: Boolean on whether to decode the logs for the transaction
        contract: The contract's instance

    Returns:
        An object with transaction details
    """
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    
    tx_details = {
        "blockNumber": receipt["blockNumber"],
        "hash": tx_hash,
        "from": receipt["from"],
        "to": receipt["to"],
        "gasPriceGwei": float(receipt["effectiveGasPrice"]/1000000000),
        "txfee": receipt["gasUsed"]*receipt["effectiveGasPrice"]/1000000000000000000,
        "logs": get_decoded_logs(abi_events, receipt, contract) if decode_logs else []
    }
    
    return tx_details

def decode_logs_data(event_params, data):
    """
    Function to decode logs data

    Args:
        event_params: Parameters of an event
        data: Data to decode

    Returns:
        Decoded data
    """
    byte_data = bytes.fromhex(data[2:])
    decodedABI = abi.decode(event_params, byte_data)
    return decodedABI

def decode_log_topic(topic, data):
    """
    Function to decode data for a topic

    Args:
        topic: Topic params
        data: Data to decode

    Returns:
        Decoded data
    """
    byte_data = bytes.fromhex(data[2:])
    decodedABI = abi.decode([topic], byte_data)
    return decodedABI[0]

def column_builder_transmissions(nop_details, transmitters):
    """
    Function to create a column list for a DataFrame for operators' submissions

    Args:
        nop_details: Details of node operators
        transmitters: Array of transmitters

    Returns:
        An array with the column names for the DataFrame
    """
    columns = ["blockNumber", "txDate", "gasPriceGwei", "fee", "timestamp", "txHash", "submitter", "aggregatedAnswer", "minAnswer", "maxAnswer"]
    
    for transmitter in transmitters:
        transmitter_name = nop_details[transmitter.lower()]["name"]
        columns.append(transmitter_name+"_answer")
        columns.append(transmitter_name+"_deviation")

    return columns

def get_transmissions(w3, provider_url, aggregator_contract_address, start_block, event_sigs, event_params, feed_path, nop_details, transmitters, abi_events, contract):
    """
    Function to get all the operator's submissions and transmissions from a block

    Args:
        w3: web3 Instance
        provider_url: The endpoint of the node to query
        aggregator_contract_address: The address of the aggregator contract
        start_block: The block from which to start getting transmissions
        event_sigs: The event signatures of the contract
        event_params: The event parameters of the contract
        feed_path: The path of the feed
        nop_details: The details of the node operators
        transmitters: A array of operators
        abi_events: Contract's ABI Events 
        contract: The contract's instance

    Returns:
        A DataFrame with operators' submissions and their deviation from the aggregated value
    """
    transactions = {}
    transmissions_df = pd.DataFrame([],columns=column_builder_transmissions(nop_details, transmitters))
    if "ethereum" in feed_path:
        transmissions = get_logs(provider_url, aggregator_contract_address, event_sigs["NewTransmission"], hex(start_block))
    else: 
        latest_block_number = w3.eth.get_block('latest')['number']
        transmissions = get_logs_throttled(provider_url, aggregator_contract_address, event_sigs["NewTransmission"], start_block, latest_block_number)

    for index,transmission in enumerate(transmissions):
        print("Ready", index, len(transmissions))

        if transmission["transactionHash"].lower() in transactions:
            tx = transactions[transmission["transactionHash"].lower()]
        else:
            tx = get_transaction_details(w3, abi_events, transmission["transactionHash"].lower(), True, contract)
            
            block = w3.eth.get_block(tx["blockNumber"])
            timestamp = block['timestamp']
            tx["timestamp"] = timestamp
            transactions[tx["hash"].lower()] = tx
            
        transmission_log = None
        #get new transmission log
        for log in tx["logs"]:
            if log["event"] == "NewTransmission":
                transmission_log = log["data"][0]["args"];
                break
                
        new_transmission = {
            "blockNumber": tx["blockNumber"], 
            "gasPriceGwei": tx["gasPriceGwei"], 
            "fee": tx["txfee"], 
            "timestamp": tx["timestamp"], 
            "submitter": tx["from"].lower(), 
            "txHash": tx["hash"].lower(),
            "aggregatedAnswer": transmission_log["answer"],
            "minAnswer": transmission_log["observations"][0],
            "maxAnswer": transmission_log["observations"][-1]
        }
        
        aggregated_answer =  transmission_log["answer"]
        obs = transmission_log["observers"]
        observers = binascii.hexlify(obs).decode('utf-8')
        observers = [int(observers[i:i+2],16) for i in range(0, len(observers), 2)]
        submissions = transmission_log["observations"]
        answers = {}
        deviations = {}

        print("Getting transmitters for "+str(tx["blockNumber"]))
        # Get transmitters at this block
        transmitters = get_transmitters_for_blocknumber(contract, tx["blockNumber"])
        print(len(transmitters))

        #initialise by all transmitters
        for transmitter in transmitters:
            transmitter_name = nop_details[transmitter.lower()]["name"]
            answers[transmitter_name] = 0
            deviations[transmitter_name] = 0

        #go through submissions and fill
        for index,answer in enumerate(submissions):
            transmitter_index = observers[index]
            transmitter = transmitters[transmitter_index]
            transmitter_name = nop_details[transmitter.lower()]["name"]
            answers[transmitter_name] = answer
            deviation = ((answer - aggregated_answer) / aggregated_answer) * 100
            deviations[transmitter_name] = abs(deviation)

        # loop through transmitters
        for answer in answers:
            new_transmission[answer+"_answer"] = answers[answer]
            new_transmission[answer+"_deviation"] = deviations[answer]
            
        transmissions_df = pd.concat([transmissions_df, pd.DataFrame([new_transmission])], ignore_index=True)
        
    
    transmissions_df["txDate"] = pd.to_datetime(transmissions_df['timestamp'], unit='s').dt.tz_localize('UTC')

    dir_path = "data/"+feed_path
    os.makedirs(dir_path, exist_ok=True)
    transmissions_df.to_csv(dir_path+'/transmissions.csv')
        
    return transmissions_df

def get_new_answers(w3, provider_url, aggregator_contract_address, start_block, event_sigs, event_params, feed_path, nop_details, transmitters, abi_events, contract):
    """
    Function to get prices of a CL feed from a start block for each block

    Args:
        w3: web3 Instance
        provider_url: The endpoint of the node to query
        aggregator_contract_address: The address of the aggregator contract
        start_block: The block from which to start getting transmissions
        event_sigs: The event signatures of the contract
        event_params: The event parameters of the contract
        feed_path: The path of the feed
        nop_details: The details of the node operators
        transmitters: A array of operators
        abi_events: Contract's ABI Events 
        contract: The contract's instance

    Returns:
        A DataFrame with CL aggregated prices for each feed for every block
    """
    answers_df = pd.DataFrame([],columns=["timestamp", "answer"])

    latest_block_number = w3.eth.get_block('latest')['number']
    new_answers = get_logs_throttled(provider_url, aggregator_contract_address, event_sigs["AnswerUpdated"], start_block, latest_block_number)
    
    decimals = contract.functions.decimals().call()

    print("got new answers "+str(len(new_answers)))
    for index,answer in enumerate(new_answers):
        # print("Ready", index, len(new_answers))
        
        value = decode_log_topic(event_params["AnswerUpdated"]["params"][1], answer["topics"][1])
        timestamp = decode_log_topic(event_params["AnswerUpdated"]["params"][2], answer["data"])

        new_answer = {
            "timestamp": timestamp, 
            "answer": value / 10 ** decimals, 
        }
            
        answers_df = pd.concat([answers_df, pd.DataFrame([new_answer])], ignore_index=True)
        
    
    answers_df["txDate"] = pd.to_datetime(answers_df['timestamp'], unit='s').dt.tz_localize('UTC')

    dir_path = "data/"+feed_path
    os.makedirs(dir_path, exist_ok=True)
    answers_df.to_csv(dir_path+'/answers.csv')
        
    return answers_df

def get_payments(w3, provider_url, aggregator_contract_address, start_block, event_sigs, event_params, feed_path, nop_details, transmitters, abi_events, contract):
    """
    Function to get all the operator's withdrawals from a start block

    Args:
        w3: web3 Instance
        provider_url: The endpoint of the node to query
        aggregator_contract_address: The address of the aggregator contract
        start_block: The block from which to start getting transmissions
        event_sigs: The event signatures of the contract
        event_params: The event parameters of the contract
        feed_path: The path of the feed
        nop_details: The details of the node operators
        transmitters: A array of operators
        abi_events: Contract's ABI Events 
        contract: The contract's instance

    Returns:
        A DataFrame with operators' withdrawals starting from the given block
    """
    transactions = {}
    payments_df = pd.DataFrame([],columns=["blockNumber", "txHash", "txTimestamp", "gasPriceGwei", "fee", "submitter", "payeeAddress", "oracleName", "amount"])
    payments = get_logs(provider_url, aggregator_contract_address, event_sigs["OraclePaid"], hex(start_block))

    for index,payment in enumerate(payments):
        print("Ready", index, len(payments))
        if "ethereum" in feed_path:
            transmitter, payee, amount = decode_logs_data(event_params["OraclePaid"]["params"], payment["data"])
        else:
            amount = decode_log_topic(event_params["OraclePaid"]["params"][2], payment["data"])
            transmitter = decode_log_topic(event_params["OraclePaid"]["params"][0], payment["topics"][1])
            payee = decode_log_topic(event_params["OraclePaid"]["params"][1], payment["topics"][2])

        if payment["transactionHash"] in transactions:
            tx = transactions[payment["transactionHash"]]
        else:
            tx = get_transaction_details(w3, abi_events, payment["transactionHash"], False, contract)
            block = w3.eth.get_block(tx["blockNumber"])
            timestamp = block['timestamp']
            tx["timestamp"] = timestamp
            transactions[tx["hash"]] = tx
            
        new_payment = {
                "blockNumber": tx["blockNumber"], 
                "txHash": tx["hash"],
                "txTimestamp": tx["timestamp"], 
                "gasPriceGwei": tx["gasPriceGwei"], 
                "fee": tx["txfee"], 
                "submitter": tx["from"].lower(), 
                "payeeAddress": payee.lower(),
                "oracleName": nop_details[transmitter.lower()]["name"],
                "amount": amount / 1000000000000000000
            }
            
        payments_df = pd.concat([payments_df, pd.DataFrame([new_payment])], ignore_index=True)
        
    payments_df["txDate"] = pd.to_datetime(payments_df['txTimestamp'], unit='s').dt.tz_localize('UTC')

    dir_path = "data/"+feed_path
    os.makedirs(dir_path, exist_ok=True)
    payments_df.to_csv(dir_path+'/payments.csv')
        
    return payments_df

def get_transmitters_for_block(w3_archive, aggregator_contract_address, aggregator_abi, block_numbers):
    """
    Function to get the operators for a feed at particular blocks

    Args:
        w3_archive: web3 archive Instance
        aggregator_contract_address: The address of the aggregator contract
        aggregator_abi: The aggregator contract's ABI
        block_numbers: The blocks at which to query the transmitters
        contract: The contract's instance

    Returns:
        A dict of the transmitters at every block starting fr
    """
    price_address = to_checksum_address(aggregator_contract_address)
    price_contract_archive = w3_archive.eth.contract(address=aggregator_contract_address, abi=aggregator_abi)
    
    transmitters = {}
    
    # for each block number
    for index, num in enumerate(block_numbers):
        transmitters[str(num)] = price_contract_archive.functions.transmitters().call(block_identifier=int(num))
        print("Got transmitters "+str(index)+"/"+str(len(block_numbers)))

    dir_path = "data/"+feed_path
    os.makedirs(dir_path, exist_ok=True)
    with open(dir_path+"/transmitters.json", "w", encoding="utf-8") as outfile:
        json.dump(transmitters, outfile, ensure_ascii=False, indent=4)
        
    return transmitters

def get_transmitters_for_blocknumber(contract, block_number):
    """
    Function to get the operators for a feed at a particular block

    Args:
        contract: The contract's instance
        block_number: The block at which to query the transmitters

    Returns:
        The transmitters at the given block
    """
    transmitters = contract.functions.transmitters().call(block_identifier=int(block_number))
    return transmitters

def get_prices_for_blocknumbers(w3_archive, aggregator_contract_address, aggregator_abi, block_numbers, feed, feed_path):
    """
    Function to get the prices for a feed at particular blocks

    Args:
        w3_archive: The contract's instance
        aggregator_contract_address: The address pf the aggregator contract
        aggregator_abi: The abi of the aggregator contract 
        block_numbers: The block numbers for which to get the prices
        feed: The feed name
        feed_path: The path of the feed

    Returns:
        A dict of prices for each block number
    """
    price_address = to_checksum_address(aggregator_contract_address)
    price_contract_archive = w3_archive.eth.contract(address=aggregator_contract_address, abi=aggregator_abi)
    decimals = price_contract_archive.functions.decimals().call()
    
    prices = {}
    
    # for each block number
    for index, num in enumerate(block_numbers):
        try:
            prices[str(num)] = price_contract_archive.functions.latestAnswer().call(block_identifier=int(num)) / (10 ** decimals)
            print("Got price "+str(index)+"/"+str(len(block_numbers)))
        except:
            print("Failed to get price for "+feed+" for block "+str(+num))

    dir_path = "data/"+feed_path+"/prices"
    os.makedirs(dir_path, exist_ok=True)
    prices = {str(k):float(v) for k,v in prices.items()}
    with open(dir_path+"/"+feed+".json", "w", encoding="utf-8") as outfile:
        json.dump(prices, outfile, ensure_ascii=False, indent=4)
        
    return prices

def column_builder_perop(transmitters):
    """
    Function to build column names for each operator

    Args:
        transmitters: Array of operators

    Returns:
        A list of column names for each operator
    """
    columns = []

    for transmitter in transmitters:
        transmitter_name = nop_details[transmitter.lower()]["name"]
        columns.append(transmitter_name+"_deviation_avg")
        columns.append(transmitter_name+"_fees")
        columns.append(transmitter_name+"_payments")

    return columns

def get_block_billing(block, billing_params):
    """
    Function to get billing parameters for a block

    Args:
        block: The block for which to get the billing parameters
        billing_params: All the billing params for each block

    Returns:
        The billing params at a particular block
    """
    last_key = ""
    for key in billing_params:
        last_key = key
        if int(block) < int(key):
            return billing_params[key]
    
    return billing_params[last_key]

def get_billing_ranges(billing_params):
    """
    Function to get ranges for billing parameters

    Args:
        billing_params: All the billing params for each block

    Returns:
        The ranges for billing parameters
    """

    # Sort the keys in ascending order
    sorted_keys = sorted(map(int, billing_params.keys()))

    # Calculate the ranges between the keys
    ranges = [{"from": sorted_keys[i-1], "to": sorted_keys[i]} for i in range(1, len(sorted_keys))]
    ranges += [{"from": sorted_keys[-1], "to": -1}]

    return ranges

def get_transmission_repayments(submissions, billing_params):
    """
    Function to get repayments for transmissions for a list of submissions

    Args:
        submissions: All the submissions to calculate repayments for it
        billing_params: The billing params to use

    Returns:
        The repayment for submissions
    """

    repayments_eth = 0
    for index, row in submissions.iterrows():
        price_paid = row['gasPriceGwei']
        # if paid more than max price in eth gwei units
        if price_paid > billing_params["maximumGasPrice"]:
            repayments_eth += (billing_params["maximumGasPrice"] / 1000000000.0) * row["gasCost"]
        else:
            repayments_eth += (price_paid/1000000000.0) * row["gasCost"]

        # if price paid is less than reasonable, give half savings
        if price_paid < billing_params["reasonableGasPrice"]:
            savings = ((billing_params["reasonableGasPrice"] - price_paid) / 1000000000.0) * row["gasCost"]
            repayments_eth += savings/2.0

    return repayments_eth * billing_params["microLinkPerEth"] / 1000000.0

def count_consecutive_missed(df, column_name):
    """
    Function to calculate consecutive missed observations

    Args:
        df: DataFrame of submissions to iterate
        column_name: Colum name of the operator's submission

    Returns:
        1. The number of missed observations
        2. The number of separate missed observations
        3. The number of separate consecutive missed observations
    """
    missed = []
    counter_missed = 0
    separate_missed_instances = []
    separate_consecutive_missed_instances = []
    prev_missed = False
    new_consecutive_missed = True
    missed_instances_changed = False
    consecutive_missed_instances_changed = False
    for index, row in df.iterrows():
        submission = row[column_name]
        if submission == 0:
            if prev_missed:
                counter_missed = counter_missed + 1
                if new_consecutive_missed:
                    consecutive_missed_instances_changed = True
                    new_consecutive_missed = False
            else:
                missed_instances_changed = True
            prev_missed = True
        else:
            counter_missed = 0
            prev_missed = False
            new_consecutive_missed = True

        missed.append(counter_missed)
        separate_missed_instances.append(int(missed_instances_changed))
        separate_consecutive_missed_instances.append(int(consecutive_missed_instances_changed))
        missed_instances_changed = False
        consecutive_missed_instances_changed = False

    return missed, separate_missed_instances, separate_consecutive_missed_instances

def calculate_estimated_earnings(nop_details, billing_params, index_withdrawal_dates, feed_details, withdrawal_range, withdrawal_block):
    """
    Function to calculate consecutive missed observations

    Args:
        nop_details: The details of node operators
        billing_params: The billing parameters for each block
        index_withdrawal_dates: The index of the withdrawal. If it is the first withdrawal, this would be 0
        feed_details: The details of the feed
        withdrawal_range: The withdrawal range with the start and end dates of the submissions in that range
        withdrawal_block: The block of the withdrawal

    Returns:
        A dict with estimated earnings for each operator
    """
    billing_ranges = get_billing_ranges(billing_params)

    # read link prices
    linkprices_filename = "data/"+feed_details["path"]+"/prices/link-usd.json"
    with open(linkprices_filename, "r") as file:
        link_prices = json.load(file)

    # read eth prices
    ethprices_filename = "data/"+feed_details["path"]+"/prices/eth-usd.json"
    with open(ethprices_filename, "r") as file:
        eth_prices = json.load(file)

    estimated_earnings = {
        "observationsCounts": {},
        "transmissionsCounts": {},
        "estimatedObservationsEarnings": {},
        "estimatedTransmissionsEarnings": {},
        "estimatedTransmissionsRepayments": {},
        "estimatedTotalEarnings": {}
    }

    for nop in nop_details:
        observations_count = 0
        transmissions_count = 0
        estimated_observations_earnings = 0
        estimated_transmissions_earnings = 0
        submissions_filename = "data/"+feed_details["path"]+"/per_op/"+nop_details[nop]["name"]+"/submissions.csv"
        if os.path.exists(submissions_filename):
            submissions = pd.read_csv(submissions_filename)

            # trim submissions to given range
            withdrawal_date_from = withdrawal_range["from"]
            withdrawal_date_to = withdrawal_range["to"]

            # if first withdrawal index
            submissions = submissions[submissions["txDate"] < withdrawal_date_to] if index_withdrawal_dates == 0 else submissions[(submissions["txDate"] < withdrawal_date_to) & (submissions["txDate"] >= withdrawal_date_from)]

            # get billing params at withdrawal block
            billing_params_range = get_block_billing(withdrawal_block, billing_params)

            # calculate gas cost from price 
            submissions["gasCost"] = submissions["fee"] / (submissions["gasPriceGwei"]/1000000000)
            
            observations_count = len(submissions)
            transmissions_in_range = submissions[submissions["submitter"] == nop]
            transmissions_count = len(transmissions_in_range)
            # get transmission repayments in link
            repayments_link = get_transmission_repayments(transmissions_in_range, billing_params_range)
            repayments_usd = repayments_link * link_prices[withdrawal_block]
            # calculate observation earnings for billing range. Divide by 1000000000 to get amount in link
            estimated_observations_earnings = observations_count * (billing_params_range["linkGweiPerObservation"] / 1000000000.0) * link_prices[withdrawal_block]
            estimated_transmissions_earnings = transmissions_count * (billing_params_range["linkGweiPerTransmission"] / 1000000000.0) * link_prices[withdrawal_block]

            estimated_earnings["observationsCounts"][nop_details[nop]["name"]] = observations_count
            estimated_earnings["transmissionsCounts"][nop_details[nop]["name"]] = transmissions_count
            estimated_earnings["estimatedObservationsEarnings"][nop_details[nop]["name"]] = estimated_observations_earnings
            estimated_earnings["estimatedTransmissionsEarnings"][nop_details[nop]["name"]] = estimated_transmissions_earnings
            estimated_earnings["estimatedTransmissionsRepayments"][nop_details[nop]["name"]] = repayments_usd
            estimated_earnings["estimatedTotalEarnings"][nop_details[nop]["name"]] = estimated_observations_earnings + estimated_transmissions_earnings + repayments_usd
        
    return estimated_earnings

def get_totals(unique_withdrawal_dates, payments, transmissions, transmitters, nop_details, feed_details):
    """
    Function to calculate consecutive missed observations

    Args:
        unique_withdrawal_dates: Array of withdrawal dates
        payments: DataFrame of payments
        transmissions: DataFrame of submissions and transmissions
        transmitters: An array of transmitters
        nop_details: The details of node operators
        feed_details: The details of the feed

    Returns:
        A dict with totals for each operator. Includes profits and observation misses
    """
    submission_dfs = []
    totals = {
        "ranges": [],
        "totals": []
    }
    for index,withdrawal_date in enumerate(unique_withdrawal_dates):
        total = {
            "deviations": {},
            "maxDeviation": {},
            "fees": {},
            "payments": {},
            "profits": {},
            "observationsCounts": {},
            "missedObservations": {},
            "transmissionsCounts": {},
            "estimatedObservationsEarnings": {},
            "estimatedTransmissionsEarnings": {},
            "estimatedTotalEarnings": {},
            "consecutiveMissedObservations": {},
            "maxConsecutiveMissedObservations": {},
            "separateMissedObservationsInstances": {},
            "separateConsecutiveMissedObservationsInstances": {},
        }
        if index == 0:
            submission_df = transmissions[transmissions["txDate"] < withdrawal_date]
            withdrawal_df = payments[payments["txDate"] <= withdrawal_date]
            range_total = {
                "from": withdrawal_df.iloc[0]["txDate"],
                "to": withdrawal_date
            }
        else:
            submission_df = transmissions[(transmissions["txDate"] < withdrawal_date) & (transmissions["txDate"] >= unique_withdrawal_dates[index-1])]
            withdrawal_df = payments[(payments["txDate"] <= withdrawal_date) & (payments["txDate"] > unique_withdrawal_dates[index-1])]
            range_total = {
                "from": unique_withdrawal_dates[index-1],
                "to": withdrawal_date
            }

        # get withdrawal block from withdrawal date
        withdrawal_block = payments[payments["txDate"]==withdrawal_date].iloc[0]["blockNumber"]
            
        if len(submission_df) > 0:
            submission_dfs.append(submission_df)
            
        #group withdrawal_df by receiver
        withdrawal_df_totals = withdrawal_df.groupby("oracleName")["usdAmount"].sum()

        # read link prices
        linkprices_filename = "data/"+feed_details["path"]+"/prices/link-usd.json"
        with open(linkprices_filename, "r") as file:
            link_prices = json.load(file)


        for transmitter in transmitters:
            transmitter_name = nop_details[transmitter.lower()]["name"]
            
            # change with actual prices
            submission_df[transmitter_name+"_fees"] = submission_df["ethPrice"] * submission_df[transmitter_name+"_fees"]
            
            total["deviations"][transmitter_name] = submission_df[transmitter_name+"_deviation"].mean()
            total["maxDeviation"][transmitter_name] = submission_df[transmitter_name+"_deviation"].max()
            total["fees"][transmitter_name] = submission_df[transmitter_name+"_fees"].sum()
            total["payments"][transmitter_name] = withdrawal_df_totals[transmitter_name] if transmitter_name in withdrawal_df_totals.index.unique() else 0
            total["missedObservations"][transmitter_name] = len(submission_df[submission_df[transmitter_name+"_answer"]==0])
            total["consecutiveMissedObservations"][transmitter_name] = len(submission_df[submission_df[transmitter_name+"_consecutiveMissed"]!=0])
            total["maxConsecutiveMissedObservations"][transmitter_name] = submission_df[transmitter_name+"_consecutiveMissed"].max()
            total["separateMissedObservationsInstances"][transmitter_name] = len(submission_df[submission_df[transmitter_name+"_separateMissed"]!=0])
            total["separateConsecutiveMissedObservationsInstances"][transmitter_name] = len(submission_df[submission_df[transmitter_name+"_separateConsecutiveMissed"]!=0])

        # Read billings
        billing_params_filename = "data/"+feed_details["path"]+"/billing_params.json"
        with open(billing_params_filename, 'r') as file:
            billing_params = json.load(file)
        # get estimated earnings per nop
        estimated_earnings = calculate_estimated_earnings(nop_details, billing_params, index, feed_details, range_total, withdrawal_block)
        # estimated_earnings = calculate_estimated_earnings2(nop_details, billing_params, unique_withdrawal_dates, index, feed_details)
        for key in estimated_earnings:
            total[key] = estimated_earnings[key]
        
        total["deviations"] = dict(sorted(total["deviations"].items(), key=lambda item: item[1], reverse=True))
        total["maxDeviation"] = dict(sorted(total["maxDeviation"].items(), key=lambda item: item[1], reverse=True))
        total["fees"] = dict(sorted(total["fees"].items(), key=lambda item: item[1], reverse=True))
        total["payments"] = dict(sorted(total["payments"].items(), key=lambda item: item[1], reverse=True))
        total["profits"] = {key: total["payments"][key] - total["fees"][key] for key in total["payments"]}
        total["profits"] = dict(sorted(total["profits"].items(), key=lambda item: item[1], reverse=True))
        total["observationsCounts"] = dict(sorted(total["observationsCounts"].items(), key=lambda item: item[1], reverse=True))
        total["missedObservations"] = dict(sorted(total["missedObservations"].items(), key=lambda item: item[1], reverse=True))
        total["consecutiveMissedObservations"] = dict(sorted(total["consecutiveMissedObservations"].items(), key=lambda item: item[1], reverse=True))
        total["maxConsecutiveMissedObservations"] = dict(sorted(total["maxConsecutiveMissedObservations"].items(), key=lambda item: item[1], reverse=True))
        total["separateMissedObservationsInstances"] = dict(sorted(total["separateMissedObservationsInstances"].items(), key=lambda item: item[1], reverse=True))
        total["separateConsecutiveMissedObservationsInstances"] = dict(sorted(total["missedObservations"].items(), key=lambda item: item[1], reverse=True))
        total["transmissionsCounts"] = dict(sorted(total["transmissionsCounts"].items(), key=lambda item: item[1], reverse=True))
        total["estimatedObservationsEarnings"] = dict(sorted(total["estimatedObservationsEarnings"].items(), key=lambda item: item[1], reverse=True))
        total["estimatedTransmissionsEarnings"] = dict(sorted(total["estimatedTransmissionsEarnings"].items(), key=lambda item: item[1], reverse=True))
        total["estimatedTransmissionsRepayments"] = dict(sorted(total["estimatedTransmissionsRepayments"].items(), key=lambda item: item[1], reverse=True))
        total["estimatedTotalEarnings"] = dict(sorted(total["estimatedTotalEarnings"].items(), key=lambda item: item[1], reverse=True))
        total["diffFromCalc"] = {key: total["estimatedTotalEarnings"][key] - total["payments"][key] for key in total["payments"]}
        total["diffFromCalc"] = dict(sorted(total["diffFromCalc"].items(), key=lambda item: item[1], reverse=True))
        total["diffFromCalcPerTransmission"] = {key: total["diffFromCalc"][key] / float(total["transmissionsCounts"][key]) for key in total["diffFromCalc"]}
        total["diffFromCalcPerTransmission"] = dict(sorted(total["diffFromCalcPerTransmission"].items(), key=lambda item: item[1], reverse=True))
        total["diffFromCalcPerObs"] = {key: total["diffFromCalc"][key] / float(total["observationsCounts"][key]) for key in total["diffFromCalc"]}
        total["diffFromCalcPerObs"] = dict(sorted(total["diffFromCalcPerObs"].items(), key=lambda item: item[1], reverse=True))
        totals["ranges"].append(range_total)
        totals["totals"].append(total)

    dir_path = "data/"+feed_details["path"]
    os.makedirs(dir_path, exist_ok=True)
    with open(dir_path+"/totals.json", "w", encoding="utf-8") as outfile:
            json.dump(totals, outfile, ensure_ascii=False, indent=4)

    return totals

def get_billing_params(w3, provider_url, aggregator_contract_address, event_sigs, event_params, feed_path, abi_events, contract):
    """
    Function to get billing parameters

    Args:
        w3: web3 Instance 
        provider_url: The endpoint of the node to query 
        aggregator_contract_address: The address of the aggregator contract 
        event_sigs: The signatures of the contract's events 
        event_params: The parameters of the contract's events
        feed_path: The path of the feed 
        abi_events: The events from the contract's ABI 
        contract: The contract's Instance

    Returns:
        A dict with billing parameters at each block for a contract
    """
    
    billing_params = {}
    billings = get_logs(provider_url, aggregator_contract_address, event_sigs["BillingSet"], "0x0")
    for index,billing in enumerate(billings):
        tx = get_transaction_details(w3, abi_events, billing["transactionHash"], False, contract)
        maximumGasPrice, reasonableGasPrice, microLinkPerEth, linkGweiPerObservation, linkGweiPerTransmission = decode_logs_data(event_params["BillingSet"]["params"], billing["data"])
        block_num = tx["blockNumber"]
        new_billing = {
            "maximumGasPrice": maximumGasPrice,
            "reasonableGasPrice": reasonableGasPrice,
            "microLinkPerEth": microLinkPerEth,
            "linkGweiPerObservation": linkGweiPerObservation,
            "linkGweiPerTransmission": linkGweiPerTransmission
        }
        billing_params[block_num] = new_billing
        print("Ready", index, len(billings))

    dir_path = "data/"+feed_path
    os.makedirs(dir_path, exist_ok=True)

    with open(dir_path+"/billing_params.json", "w", encoding="utf-8") as outfile:
            json.dump(billing_params, outfile, ensure_ascii=False, indent=4)
        
    return billing_params