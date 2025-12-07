import asyncio
import httpx


async def fetch():
    client = httpx.AsyncClient()
    response = await client.request(
        method="GET",
        url="https://127.0.0.1:8000/add",
        params={
            "a": "1",
            "b": "2"
        },
        headers={
            "accept": "application/json"
        }
    )

    print(response.text)

    await client.aclose()

asyncio.run(fetch())
