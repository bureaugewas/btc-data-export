import json
import os
import config
import asyncio
import asyncssh
import paramiko
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

connection_string = config.connection_string

# Try to connect to local bitcoin node
try:
    # Try to connect to the local Bitcoin node
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(config.host, config.port, config.username, config.password)

    # If the connection is successful, print a success message
    print("Connected to the Bitcoin node")

except paramiko.AuthenticationException:
    # Handle authentication errors
    print("[!] Authentication failed. Check your username and password.")
except paramiko.SSHException as e:
    # Handle SSH connection errors
    print(f"[!] SSH connection error: {e}")
except Exception as e:
    # Handle other exceptions
    print(f"[!] An error occurred: {e}")
finally:
    # Ensure the SSH connection is closed, whether it succeeded or failed
    if ssh:
        ssh.close()


# Get detailed block data based on block hash from local node
async def async_ssh_block_request(command):
    async with asyncssh.connect(config.host, username=config.username, password=config.password) as conn:
        result = await conn.run(f'docker exec c10513d3caeb bitcoin-cli {command}',
                                check=True)
        return result.stdout


# Retrieve block data based on hash
def get_block_data(i):
    block_hash = str(asyncio.get_event_loop().run_until_complete(
        async_ssh_block_request(f'getblockhash {i}'))).rstrip('\n')
    try:
        block_data = json.loads(asyncio.get_event_loop().run_until_complete(
            async_ssh_block_request(f'getblock {block_hash} 2')))

    except:
        ssh.close()
        ssh.connect(config.host, config.port, config.username, config.password)
        block_data = json.loads(asyncio.get_event_loop().run_until_complete(async_ssh_block_request(block_hash)))
        print('excepted')
    return block_data

# TODO: move azure utils to seperate file
def upload_file_to_blob(container_name, blob_name, file_path):

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Create a BlobClient for the file you want to upload
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the file to Azure Blob Storage
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)

    print(f"File {blob_name} uploaded to {container_name} container.")


# TODO: move azure utils to seperate file
def blob_exists(container_name, blob_name):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # List blobs in the container
        blob_list = container_client.list_blobs()

        # Check if the desired blob name exists in the list of blob names
        for blob in blob_list:
            if blob.name == blob_name:
                return True

        # If the loop completes and the blob is not found, return False
        return False

    except Exception as e:
        print(f"Error checking blob existence: {e}")
        return False


# Save block data as is in json format
def save_block_data_json(i, block_data, output_dir):
    reupload = False

    # Define the file path for the JSON file
    file_path = os.path.join(output_dir, f'{i}_block.json')

    try:
        # Save the block_data to the JSON file
        with open(file_path, 'w') as json_file:
            json.dump(block_data, json_file)
        #print(f"Saved block {i} data to {file_path}")
    except Exception as e:
        print(f"Error saving block {i} data: {e}")

    # Prevent file from being reuploaded
    if reupload == False and blob_exists('btc-block-data',f'json/{i}_block.json'):
        print(f'file already exists: json/{i}_block.json')
    else:
        upload_file_to_blob('btc-block-data',f'json/{i}_block.json',file_path)


# Save block data in relational format to csv
def save_block_data_csv(block_height, block_data, output_dir):

    # Clean block data
    block_data_clean = block_data.copy()
    transaction_data = []
    for key, value in block_data.items():
        if key == 'tx':
            transaction_data = value
            del block_data_clean[key]
        else:
            pass

    # Add block id to block data
    block_data_clean['block_id'] = block_height

    # Normalize block data
    df_block = pd.json_normalize(block_data_clean)

    # Clean transaction data
    transaction_data_clean = []
    vin_data_clean = []
    vout_data_clean = []
    for transaction in transaction_data:
        transaction_clean = transaction.copy()
        for key, value in transaction.items():
            if key == 'vin':
                vin_data_clean.append(value)
                del transaction_clean[key]
            if key == 'vout':

                # Add txid to vout data
                for r in value:
                    r['txid'] = transaction['txid']
                vout_data_clean.append(value)
                del transaction_clean[key]
            else:
                pass

        # Add block id to transaction data
        transaction_clean['block_id'] = block_height
        transaction_data_clean.append(transaction_clean)

    # Normalize transaction data
    df_transaction = pd.json_normalize(transaction_data_clean)

    # Clean vin transactions
    vin_tx_clean = []
    for vin in vin_data_clean:
        for vi in vin:
            vin_tx_clean.append(vi)

    # Clean vout transactions
    vout_tx_clean = []
    for vout in vout_data_clean:
        for vo in vout:
            vout_tx_clean.append(vo)

    # Normalize vin/vout transactions
    df_vin = pd.json_normalize(vin_tx_clean)
    df_vout = pd.json_normalize(vout_tx_clean)

    try:
        # Save the block_data to the JSON file
        df_block.to_csv(f'{output_dir}/block/{block_height}_block.csv', mode='w', index=False)
        df_transaction.to_csv(f'{output_dir}/tx/{block_height}_tx.csv', mode='w', index=False)
        df_vin.to_csv(f'{output_dir}/vin/{block_height}_vin.csv', mode='a', index=False)
        df_vout.to_csv(f'{output_dir}/vout/{block_height}_vout.csv', mode='w', index=False)
        print(f"Saved block {block_height} data to {output_dir}")
    except Exception as e:
        print(f"Error saving block {block_height} data: {e}")


def start_main(output_dir):
    start_height = 200000
    increment = 10

    # Iterate through blocks
    for i in range(start_height, start_height+increment):
        block_data = get_block_data(i)
        save_block_data_json(i, block_data, output_dir+'/json')
        #save_block_data_csv(i, block_data, output_dir+'/csv')


def __init__():
    # Create folder structure
    output_dir = '../btc-data-to-file/block-data'
    if not os.path.exists(f'{output_dir}'):
        os.makedirs(f'{output_dir}/json/block')
        os.makedirs(f'{output_dir}/csv/block')
        os.makedirs(f'{output_dir}/csv/tx')
        os.makedirs(f'{output_dir}/csv/vin')
        os.makedirs(f'{output_dir}/csv/vout')
    start_main(output_dir)


__init__()

