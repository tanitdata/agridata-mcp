import asyncio
import json
from tanitdata.ckan_client import CKANClient

async def main():
    client = CKANClient()
    
    print("Searching for 'incendies'...")
    res_fires = await client.package_search(query="incendies", rows=5)
    print(json.dumps(res_fires, indent=2))
    
    print("Searching for 'bois' (wood)...")
    res_wood = await client.package_search(query="bois", rows=5)
    print(json.dumps(res_wood, indent=2))
    
    print("Searching for 'exportation'...")
    res_export = await client.package_search(query="exportation", rows=5)
    print(json.dumps(res_export, indent=2))
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
