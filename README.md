# on-chain-data-collector
Tool for collecting/crawling on-chain data of any evm chains

## Installation of virtual environment(optional)
Works with python > 3.9.1 

```bash
pyenv virtualenv 3.9.1 on-chain-crawler
```

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install packages.

```bash
pip install -r requirements.txt
```

## Usage (Example)
You have to input the name of configuration file to an argparser. It is like:

```bash
python main.py --setting_filename "setting/degen_transfer/default_setting.json"
```

## Change the settings
In setting/project/*.json' file, you can change the paths of output files, node url, token address, etc. 

- project: "Degen" // name of the project, does not have to correspond to any file
- abi_file: "abi/erc20_abi.json" // file path to the abi file
- from_block: 8000000 // block number to start scanning
- to_block: 12000000 // block number to end scanning
- node_url: "https://mainnet.base.org" // rpc node url
- contract_address: "0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed" // token address
- bool_tx_receipt: false // true if you want to get the transaction receipt of each event (makes the speed very slow)
- bool_tx: false // // true if you want to get the information about transaction of each event (makes the speed very slow)
- output_csv_filename: "degen/transfer.csv" // output file name
- event_type: "Transfer" // event type (must be defined in the abi file)