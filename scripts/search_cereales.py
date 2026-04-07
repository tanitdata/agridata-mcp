import asyncio
import json
from tanitdata.ckan_client import CKANClient

async def main():
    client = CKANClient()
    print("Searching for 'cereales'...")
    res = await client.package_search(query="cereales", rows=10)
    print(json.dumps(res, indent=2))
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
