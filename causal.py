from flask import Flask, request, abort
from kvs import Kvs, KvsNode, EMPTY_NODE
from typing import Callable, Any
import asyncio
import sys
from time import sleep, time
import httpx
import json
from background import Executor, broadcastAll
from operations import OperationGenerator, Operation
from consistent_hashing import HashRing
from key_reshuffle import ViewType

# collection of kv

def RelevantCausalMetaDataToSet(l: list[str] | None, key: str) -> set[str]:
    if l is None:
        return set()
    return set(filter(lambda op: Operation.fromString(op).key == key, l))

async def getMissingDependencies(md: Operation, nodes: list[str], data: Kvs):

    def putResInData(res, _code, _sender) -> KvsNode:
        res = json.loads(res)
        node = KvsNode.fromDict(res)
        return node
    print("missing dependency: ", md.key, flush=True)
    retDict: dict[str, KvsNode] = await broadcastAll("GET", nodes, f"/keys/{md.key}", {}, 1, putResInData)
    
    # put all nodes in kvs, let kvs figure out which one to keep lol
    for node in retDict.values():
        if node is EMPTY_NODE or node is None:
            print("hi")
            continue
        data.put(md.key, node)

async def getData(key: str, request: dict, *, nodes: list[str], data: Kvs, hashRing: HashRing, associatedNodes: ViewType) -> tuple[dict, int]:
    tInit = time()

    causalMetaData:list[str] = request.get("causal-metadata", {}).get("ops", [])
    causalMetaDataSet = set(causalMetaData if causalMetaData else [])
    requestDependsOn = RelevantCausalMetaDataToSet(causalMetaData, key)
    tries = 0
    while time() - tInit < 20:

        node = data.get(key)

        nodeDependsOn = set([*map(repr, node.dependencies)])

        # if node is not EMPTY_NODE:
        #     nodeDependsOn.add(repr(node.operation))

        #find missing dependencies
        missingDependencies = requestDependsOn.difference( data.opsSeen() )

        if len(missingDependencies) == 0:
            newCausal = list(nodeDependsOn.union(causalMetaDataSet))
            if node is EMPTY_NODE or node.value == None:
                return {
                    "causal-metadata": {"ops": newCausal}
                }, 404
            return {
                "val": node.value,
                "causal-metadata": {"ops": newCausal}
            }, 200
        
        async with asyncio.TaskGroup() as tg:
            for dependency in missingDependencies:
                op = Operation.fromString(dependency)
                if op.key != key:
                    continue #skip keys that arent this key
                keyShardId, hash = hashRing.assign(key)
                print("SHARDID of missing dependency = ", keyShardId)
                tg.create_task(getMissingDependencies(op, associatedNodes[keyShardId], data))
        #now we have our dependencies, go back to top of loop and try again
        if tries != 0:
            await asyncio.sleep(0.5)
        tries += 1
    return {"error": "timed out while waiting for depended updates"}, 500
     
    



    

    
    


def putData(key: str, request: dict, *, data: Kvs, executor: Executor, nodes: list[str]) -> tuple[dict, int]:
    val = request.get("val")
    msTimestamp=request.get("timestamp")
    if val == None:
        return {"error": "bad request"}, 400
    if len(json.dumps(val)) > 1000000:
        return {"error": "val too large"}, 400
    metadata = request.get("causal-metadata", {}) or {}
    if metadata is None:
        return {"error": "bad request"}, 400

    reqCausal = []
    ops = metadata.get("ops")
    reqCausal.extend(ops if ops else [])

    op = Operation.fromString(request["operation"])
    node = KvsNode(request["val"], operation=op, msSinceEpoch=msTimestamp, dependencies=[*map(Operation.fromString, reqCausal)])
    isNew = data.put(key, node)
    code = 201 if isNew else 200
    executor.run(
        broadcastAll( "PUT", nodes, f"/keys/{key}", node.asDict(), timeout=1 )
    )

    # new array because node owns the old array
    sendBackCausal=[repr(op)]
    sendBackCausal.extend(reqCausal)
    return {
        "causal-metadata": {"ops": sendBackCausal},
        "replaced": not isNew
        }, code

def deleteData(key: str, request: dict, *, data: Kvs, executor: Executor, nodes: list[str]) -> tuple[dict, int]:
    msTimestamp = time() * 1000
    metadata = request.get("causal-metadata", {})
    reqCausal = []
    if metadata:
        ops = metadata.get("ops")
        reqCausal.extend(ops if ops else [])

    op = Operation.fromString(request["operation"])
    success = data.delete(key)
    code = 200 if success else 404
    executor.run(broadcastAll("DELETE", nodes, f"/keys/{key}", "", timeout=1))
    reqCausal.append(repr(op))
    return {"causal-metadata": {"ops": reqCausal}}, code


def update_view_data(view, data: Kvs, executor: Executor, nodes: list[str]) -> int:
    executor.run(
        broadcastAll("PUT", nodes, f"/update_kvs", {"view": view,"data": data}, timeout=1))
    return 200
