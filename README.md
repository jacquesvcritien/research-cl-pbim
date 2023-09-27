# Investigating Performance-Based Incentive Mechanisms in Chainlink

This is the repository for the codebase of the Dissertation for the Masters in Blockchain and DLTs by Jacques Vella Critien

## File Structure

- <b>Analysis.ipynb</b>: This is a Jupyter notebook with the Analysis
- <b>binance-credentials.sample.json</b>: This is a sample JSON configuration file which includes Binance's credentials. This should be copied to binance-credentials.json
- <b>config.sample.json</b>: This is a sample JSON configuration file. This should be copied to config.json
- <b>binance-data-getter.py</b>: This is a script to get Binance prices.
- <b>cl-price-getter.py</b>: This is a script to get Chainlink's prices for a feed.
- <b>data-getter.py</b>: This is a script to get Chainlink's data such as submissions and withdrawals of operators.
- <b>helper.py</b>: This contains helper functions used throughout the aforementioned files
- <b>abi</b>: This directory contains the ABI files for the contracts
- <b>data</b>: This directory contains the data collected from the code.
    - <b>binance</b>: This directory contains prices from Binance
    - <b>ethereum/mainnet</b>: This directory contains prices from CL feeds on Ethereum mainnet
    - <b>polygon</b>: This directory contains prices from CL feeds on Polygon
    - <b>feeds.json</b>: This json file contains all the feeds offered by Chainlink
    - <b>oracle_counts.json</b>: This json file contains the feed counts for each operator

For each feed in <b>data/ethereum/mainnet</b> and </b>data/polygon</b>, one is able to find the following files:
- <b>per_op</b>: A directory containing the submissions and withdrawals of each operator
- <b>prices</b>: A directory containing the prices related to this feed
- <b>answers.csv</b>: This contains the prices of the feed
- <b>billing_params.json</b>: This contains the billing parameters for this feed
- <b>nops.json</b>: This contains the details of operators
- <b>payments.csv</b>: This contains all the withdrawals for this feed
- <b>transmissions.csv</b>: This contains all the submissions and transmissions for this feed


## How to run

#### Creating the config files

1. Copy <b>binance-credentials.sample.json</b> and <b>config.sample.json</b> to <b>binance-credentials.json</b> and <b>config.json</b> respectively.
2. Change the files as needed

#### To get the Prices from Binance

1. Change <b>$FEED</b> to any feed like <b>LINKETH</b>
1. Change <b>$START_DATE</b> to any date like <b>2021-01-01</b>
1. Change <b>$END_DATE</b> to any date like <b>2023-01-01</b>

```bash
python3 binance-data-getter.py binance-data-getter.py $FEED $START_DATE $END_DATE
```

#### To get the Prices from Chainlink

1. Change <b>$NETWORK</b> to any feed like <b>ethereum</b>
1. Change <b>$FEED</b> to any feed like <b>link-eth</b>
1. Change <b>$START_DATE</b> to any date like <b>2023-01-01</b>

```bash
python3 cl-price-getter.py $NETWORK $FEED $START_DATE
```

#### To get the submissions and withdrawals from Chainlink for a feed

1. Change <b>$NETWORK</b> to any feed like <b>ethereum</b>
1. Change <b>$FEED</b> to any feed like <b>link-eth</b>
1. Change <b>$START_DATE</b> to any date like <b>2023-01-01</b>

```bash
python3 data-getter.py $NETWORK $FEED $START_DATE
```

    


    