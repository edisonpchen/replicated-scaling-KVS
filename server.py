
from flask import Flask, request, abort
import os
import sys
import requests
import pickle
from time import time
from random import randrange
import asyncio
import json
import httpx

from kvs import Kvs, KvsNode, getLargerNode
from background import Executor, broadcastOne, broadcastAll
from operations import OperationGenerator, Operation
from causal import getData, putData, deleteData
from consistent_hashing import HashRing
from typing import Coroutine, Any
from key_reshuffle import remove_shards, add_shards, solveViewChange 
from merkle import MerkleTree, MerkleTreeDifferenceFinder, Payload
from uuid import uuid1


# need startup logic when creating a replica (broadcast?)
NAME = os.environ.get('ADDRESS')  # get IP and port
if not NAME:  # if no ADDRESS exit with return value '1'
    sys.exit('1')
PORT = 8080
DATA = Kvs()
BGE = Executor()
OPGEN = OperationGenerator(NAME)

nodes = [] # hold list of node in the cluster
operations = []
initialized = False
associated_nodes:dict[str, list[str]] = {} # hold shard id and nodes associated with it
current_shard_id = None
reshuffle = False
merkleTreeCache: dict[str, MerkleTreeDifferenceFinder] = {}
hashRing = HashRing(2**64, 2000)

app = Flask(__name__)

# kvs/admin/view - GET, PUT, DELETE
@app.route('/kvs/admin/view', methods= ['PUT'])
async def putview():
    global nodes, initialized
    myjson = request.get_json(silent=True)
    if (myjson == None):
        return {"error": "bad request"}, 400
    if (myjson.get("nodes") == None):
        return {"error": "bad request"}, 400
    if (myjson.get("num_shards") == None):
        return {"error": "bad request"}, 400
    initialized = True


    numshards = int(myjson["num_shards"]) #Number of Shards
    nodelist = myjson["nodes"] #(Temporary Variable) List of nodes

    if numshards > len(nodelist):
        return {"bruh": "too many nodes"}, 400

    global associated_nodes, current_shard_id, nodes

    # if len(associated_nodes):
    #     old_nodelist = []
    #     for key in associated_nodes:
    #         for n in associated_nodes[key]:
    #             old_nodelist.append(n)

        # num_old_shards = len(associated_nodes.keys())
        # # remove a shard -- move nodes in that shard to another shard
        # if numshards < num_old_shards:
        #     current_shard_id = remove_shards(num_old_shards, numshards, associated_nodes, hashRing, nodelist, NAME)

        # # FIXME: need to do
        # elif numshards > num_old_shards:
        #     add_shards(num_old_shards, numshards, associated_nodes, hashRing, nodelist, NAME)
        # else:
        #     # when list of new nodes is different than list of old nodes but num_shard stay the same
        #     print()
    associated_nodes, nodesToInform = solveViewChange(associated_nodes, nodelist, numshards)
    assert len(associated_nodes) == numshards
    for shardId, nodesInShard in associated_nodes.items():
        if NAME in nodesInShard:
            current_shard_id = shardId
            nodes = nodesInShard.copy()
    nodes.remove(NAME)


    await broadcastAll("PUT", list(nodesToInform), "/update_view", data=associated_nodes, timeout=5)

    return "OK", 200

@app.route('/kvs/admin/view', methods=['GET'])
def getview():
    l = []
    for shard in associated_nodes:
        l.append({'shard_id': str(shard), 'nodes': associated_nodes[shard]})
    return ({'view': l}), 200

@app.route('/kvs/admin/view', methods=['DELETE'])
def deleteFromViewEndpoint():
    return delete_node()


def delete_node():
    global initialized, DATA
    if not initialized:
        return {"error": "uninitialized"}, 418

    nodes.clear()
    associated_nodes.clear()
    hashRing.clear()
    DATA = Kvs()
    initialized = False
    return "", 200

def checkjson(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True

@app.route('/update_view', methods= ['PUT'])
async def update_kvs_view():
    global DATA, nodes, initialized, associated_nodes, current_shard_id, hashRing, reshuffle
    reshuffle = True
    d = request.json
    associated_nodes = d
    hashRing.clear()
    current_shard_id = None
    
    for k,v in associated_nodes.items():
        hashRing.add_shard(k)
        if NAME in v:
            current_shard_id = k
    if current_shard_id:
        nodes = associated_nodes[current_shard_id].copy()
        nodes.remove(NAME)
    dataToSend: dict[str, dict[str, dict]] = {}
    for k in DATA.get_all_keys():
        shard_id, hash = hashRing.assign(k)
        if shard_id == current_shard_id:
            print(shard_id, current_shard_id)
            continue
        if shard_id in dataToSend:
            dataToSend[shard_id].update( {k: DATA.get(k).asDict()} )
        else:
            dataToSend[shard_id] = {k: DATA.get(k).asDict()}
    print("dataToSend=",dataToSend, flush=True)
    futures: list[Coroutine[Any, Any, tuple[str, int] | None]] = []
    for shardId, shardData in dataToSend.items():
        futures.append(asyncio.create_task( broadcastOne(
            "PUT",
            associated_nodes[shardId],
            "/reshuffle",
            shardData,
            20,
        ) ))
    if futures:
        done, pending = await asyncio.wait(futures, timeout=5)
        print("d,p = ", done, pending) #debug
    # when node is not in any shard -- send keys away before deleting
    if current_shard_id == None:
        delete_node()
        return "OK", 200

    initialized = True
    reshuffle = False
    return "OK", 200

@app.route('/reshuffle', methods=['PUT'])
def reshuffle_key():
    data = request.json
    for d in data.keys():
        kvs_node = KvsNode.fromDict(data[d])
        DATA.put(d, kvs_node)

    return "OK"

# kvs/data/<KEY> - GET, PUT, DELETE

@app.route("/keys/<key>", methods=["GET"])
def getKey(key):
    key = DATA.get(key)
    return key.asDict()

@app.route("/keys/<key>", methods=["PUT"])
def putKey(key):
    reqDict = request.get_json(silent=True)
    assert reqDict
    node = KvsNode( reqDict["value"],
        operation=Operation.fromString(reqDict["operation"]),
        msSinceEpoch=int(reqDict["timestamp"]),
        dependencies=[*map(Operation.fromString, reqDict["dependencies"])]
    )
    DATA.put(key, node)
    return ":)"

@app.route('/keys/<key>', methods=["DELETE"])
def delete_key(key):
    DATA.delete(key)
    return ":)"



@app.route("/kvs/data/<key>", methods=["GET", "PUT", "DELETE"])
async def keyEndpoint(key: str):
    if not initialized:
        return {"error": "uninitialized"}, 418
    if request.get_json(silent=True) == None:
        return {"error": "bad request"}, 400

    #get url for every node in correct shard
    shardId, keyHashesTo = hashRing.assign(key)
    addresses = associated_nodes[shardId]
    proxyData = request.json
    proxyData["timestamp"] = time() * 1000
    proxyData["operation"] = repr(OPGEN.nextName(key))
    print(addresses, flush=True)
    res = await broadcastOne(request.method, addresses, f"/proxy/data/{key}", proxyData, 20)
    if res is None:
        return {"error": "upstream down", "upstream": {"shard_id": shardId, "nodes": [addresses]}}, 503
    return res

@app.route("/proxy/data/<key>", methods=["GET", "PUT", "DELETE"])
async def dataRoute(key):
    if not initialized:
        return {"error": "uninitialized"}, 418
    if request.get_json(silent=True) == None:
        return {"error": "bad request"}, 400

    match request.method:
        case "GET":
            assert hashRing.assign(key)[0] == current_shard_id
            res = await getData(key, request.json, nodes=nodes, data=DATA, hashRing=hashRing, associatedNodes=associated_nodes)
            return res
        case "PUT":
            res = putData(key, request.json, data=DATA, nodes=nodes, executor=BGE)
            return res
        case "DELETE":
            return deleteData(key, request.json, data=DATA, nodes=nodes, executor=BGE)
        case _default:
            abort(405)


# kvs/data - GETs
@app.route("/kvs/data", methods=["GET"])
async def get_keys():
    if not initialized:
        return {"error": "uninitialized"}, 418
    if request.get_json(silent=True) == None:
        return {"error": "bad request"}, 400
    new_keys = []
    metadata = request.json['causal-metadata']
    operations = set()
    if "ops" in metadata:
        operations = set(metadata["ops"])

    count = 0
    for key in DATA.get_all_keys():
        if hashRing.assign(key)[0] != current_shard_id:
            continue
        res, code = await getData(key, request.json, nodes=nodes, data=DATA, hashRing=hashRing, associatedNodes=associated_nodes)
        if code == 500:
            return res
        if code == 200:
            count += 1
            new_keys.append(key)
            operations.update(res.get('causal-metadata', {}).get("ops", []))
    return {
        'shard_id': current_shard_id,
        "count" : count,
        "keys" : new_keys,
        "causal-metadata" : {"ops": list(operations)}
    }, 200


async def sendGossip(toNode:str, content: dict, uuid: str) -> list[int]:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.put(f"http://{toNode}/gossip",json={"data": content, "id": uuid}, timeout=2)
            res: list = response.json()
        except httpx.TimeoutException:
            res = []
        return res

async def gossipProtocol(differenceFinder: MerkleTreeDifferenceFinder, node: str):
    row = 1
    uuid = str(uuid1())
    res = None
    while True:
        outgoing = differenceFinder.dumpNextPyramidRow(row, res)
        res = await sendGossip(node, outgoing, uuid)
        row += 1
        if not res:
            return

async def gossip():
    while True:
        while reshuffle:
            await asyncio.sleep(0.5)

        await asyncio.sleep(1)
        # don't send anything if there is no available nodes or no data
        if len(nodes) == 0:
            continue
        if len(DATA) == 0:
            continue

        nums = [randrange(len(nodes)) for _ in range(min(len(nodes), 3))]
        nodesToSendTo = [nodes[num] for num in nums]
        merkleTree = MerkleTree()
        for key in DATA.get_all_keys():
            merkleTree.insert(Payload(key, json.dumps(DATA.get(key).asDict())))
        differenceFinder = MerkleTreeDifferenceFinder(merkleTree)

        tasks: list[asyncio.Task[tuple[str, list[int]]]] = []
        for node in nodesToSendTo:
            tasks.append( asyncio.create_task( gossipProtocol(differenceFinder, node) ) )
        await asyncio.wait(tasks, timeout=5)

def getMerkleFromCache(uuid: str) -> MerkleTreeDifferenceFinder:
    global merkleTreeCache
    if uuid in merkleTreeCache:
        return merkleTreeCache[uuid]
    else:
        merkleTree = MerkleTree()
        for key in DATA.get_all_keys():
            merkleTree.insert(Payload(key, json.dumps(DATA.get(key).asDict())))
        mtdf = MerkleTreeDifferenceFinder(merkleTree)
        merkleTreeCache[uuid] = mtdf
        return mtdf
def finishedWithMerkle(uuid: str):
    global merkleTreeCache
    if uuid in merkleTreeCache:
        del merkleTreeCache[uuid]

@app.route('/gossip', methods=['PUT'])
def update_tree():
    global DATA

    # receive tree and compare it to the
    uuid = request.json["id"]
    incoming = request.json["data"]
    differenceFinder = getMerkleFromCache(uuid)
    res = differenceFinder.compareForDifferences(incoming)
    for d in differenceFinder.getResult():
        key = d["key"]
        val = d["val"]
        DATA.put(key, KvsNode.fromDict(json.loads(val)))
    if not res:
        finishedWithMerkle(uuid)
    return res


if __name__ == "__main__":
    BGE.run(gossip())
    app.run(host='0.0.0.0', port=PORT, debug=True)
