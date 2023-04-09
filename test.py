import setting
import requests
from pytonlib import TonlibClient
import asyncio

cfg_url = setting.tonclient_url
cfg = requests.get(cfg_url).json()
client = TonlibClient(ls_index=0,config=cfg,keystore='.keystore')

async def get_balance(ACCOUNT) -> int:
    status = await client.raw_get_account_state(ACCOUNT)
    balance = status['balance']
    print(balance)
    print(type(balance))
    return balance

async def client_init():    
    await client.init()
    
asyncio.get_event_loop().run_until_complete(client_init())

asyncio.get_event_loop().run_until_complete(get_balance(ACCOUNT=setting.ACCOUNT))
