import asyncio
from threading import Thread
import httpx
from typing import Any, Callable
import unittest

#each thread needs one Executor for sending broadcast messages in the background
class Executor:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        thread = Thread(target=__class__._run, args=[self.loop], daemon=True)
        thread.start()

    def _run(loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()
    
    def run(self, asyncFuncRet):
        return asyncio.run_coroutine_threadsafe(asyncFuncRet, self.loop)



# callback is response body, status code, sender address
async def sendWithCallback(method: str, address:str, endpoint: str, data, timeout, callback: Callable[[str, int, str], Any] | None, responses: dict | None = None):
    url = f"http://{address + endpoint}"
    async with httpx.AsyncClient() as client:
        try:
            res = await client.request(method, url, json=data, timeout=timeout)
        except httpx.RequestError:
            if responses:
                responses[address] = None
            return
        code = res.status_code
        body = res.text
        if callback is not None:
            callBackResponse = callback(body, code, address)
            if responses is not None:
                responses[address] = callBackResponse

async def sendAsync(method: str, address:str, endpoint: str, data, timeout) -> tuple[str, int]:
    url = f"http://{address + endpoint}"
    async with httpx.AsyncClient() as client:
        try:
            res = await client.request(method, url, json=data, timeout=timeout)
        except httpx.RequestError:
            return None, -1
        code = res.status_code
        body = res.text
        return body, code

async def broadcastAll(method, addresses: list[str], endpoint, data, timeout, callback: Callable[[str, int, str], Any] | None = None) -> dict:
    res = {}
    async def sendBound(address):
        await sendWithCallback(method, address, endpoint, data, timeout, callback, res)
    async with asyncio.TaskGroup() as tg:
        for addy in addresses:
            tg.create_task(sendBound(addy))
    return res

async def broadcastOne(method, addresses: list[str], endpoint: str, data, timeout) -> tuple[str, int] | None:
    res = {}
    async def sendBound(address):
        return await sendAsync(method, address, endpoint, data, timeout)
    futures = [asyncio.create_task( sendBound(address) ) for address in addresses]
    while True:
        done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
        while done:
            first = done.pop()
            if first and first.done():
                if first.result()[1] in [200, 201]:
                    return first.result()
        if not pending:
            return None
        futures = pending

class Tests(unittest.IsolatedAsyncioTestCase):
    async def testOne(self):
        ips = ["localhost:8082"]
        res = await broadcastOne("GET", ips, "/", {}, 10)
        print(res, flush=True)

if __name__ == "__main__":
    unittest.main()