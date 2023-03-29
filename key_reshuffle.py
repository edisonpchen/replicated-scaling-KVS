import requests
import json
import math

from consistent_hashing import HashRing
from kvs import Kvs, KvsNode

############################
# FIXME: currently assume that number of nodes hasn't changed
# find the amount of shards need to be remove
# get list of nodes from removed shard
# add them to the remaining shards
# send every nodes the new view
############################


ViewType = dict[str, list[str]]

def generateEmptyView(nShards: int) -> ViewType:
    return {f"shard{i}": [] for i in range(nShards)}

#returns new view
def solveViewChange(oldView: ViewType, nodesInNewView: list[str], nShards: int ) -> tuple[ViewType, set[str]]:
    newView = generateEmptyView(nShards)

    nodesInNewViewSet = set(nodesInNewView)
    nodesInOldViewSet: set[str] = set()
    for nodes in oldView.values():
        nodesInOldViewSet.update(nodes)
    
    nodesToAdd = nodesInNewViewSet.difference(nodesInOldViewSet)
    nodesToRemove = nodesInOldViewSet.difference(nodesInNewViewSet)

    minNodesPerShard = len(nodesInNewView) // nShards
    shardsWithExtraNodeCount = len(nodesInNewView) % nShards
    maxNodesPerShard = minNodesPerShard + (1 if shardsWithExtraNodeCount else 0)
    for shardId in oldView:
        if shardId not in newView:
            nodesToAdd.update( [n for n in oldView[shardId] if n not in nodesToRemove] )
            continue
        newView[shardId] = [n for n in oldView[shardId] if n not in nodesToRemove]
        while len(newView[shardId]) > maxNodesPerShard:
            nodesToAdd.add(newView[shardId].pop())
        if len(newView[shardId]) == maxNodesPerShard and shardsWithExtraNodeCount == 0:
            nodesToAdd.add(newView[shardId].pop())
        elif len(newView[shardId]) == maxNodesPerShard:
            shardsWithExtraNodeCount -= 1

    for node in nodesToAdd:
        addedNode = False
        for shard in newView.values():
            if len(shard) < minNodesPerShard:
                shard.append(node)
                addedNode = True
                break
        if addedNode:
            continue
        for shard in newView.values():
            if len(shard) < maxNodesPerShard:
                shard.append(node)
                addedNode = True
                break
        if not addedNode:
            print("here officer!", node, newView)
            assert False

    assert len(newView) == nShards
    return newView, nodesInNewViewSet.union(nodesInOldViewSet)
    

def remove_shards(num_old_shards: int , numshards: int, associated_nodes: dict, hashRing: HashRing, nodelist: list, NAME: str):
    num_shard_need_to_rm = num_old_shards - numshards

    min_shard_id = [k for k in sorted(associated_nodes, key=lambda k:len(associated_nodes[k]))][:num_shard_need_to_rm]
            
    for id in min_shard_id:
        nodes_need_to_move += associated_nodes[id]
        associated_nodes.pop(id, None)
        hashRing.remove_shard(id)

    shard_id = associated_nodes.keys()
    y = 0
    num_node_in_shard = math.floor(len(nodelist) / numshards)
    for n in nodes_need_to_move:
        while (len(associated_nodes[shard_id[y]]) > num_node_in_shard):
            y += 1
            if y == numshards:
                y = 0
            
        associated_nodes[shard_id[y]].append(n)
        if n == NAME:
            current_shard_id = shard_id[y]
        y += 1
        if y == numshards:
            y = 0

    for k in associated_nodes.keys():
        for n in associated_nodes[k]:
            if n == NAME: 
                continue
            url = f'http://{n}/update_view'
            requests.put(url, json=json.dumps(associated_nodes), timeout=1)
    
    return current_shard_id


def add_shards(num_old_shards: int, numshards: int, associated_nodes: dict, hashRing: HashRing, nodelist: list, NAME:str):
    num_shard_need_to_add = numshards - num_old_shards

    for i in range(num_shard_need_to_add):
        new_shard_id = 'shard' + str(num_old_shards + i)
        associated_nodes[new_shard_id] = associated_nodes.get(new_shard_id, [])
        hashRing.add_shard(new_shard_id)

    num_node_in_shard = math.floor(len(nodelist) / numshards)  # the min number of nodes per shard

    new_nodes_need_to_add = []
    old_node_need_rm = []
    old_nodelist = []
    for k in associated_nodes.keys():
        for n in associated_nodes[k]:
            old_nodelist.append(n)

    # in the new node list but not in the old list -- need to add to associated_nodes
    for n in nodelist:
        if n not in old_nodelist:
            # add new nodes to associated_nodes
            keys = list(associated_nodes.keys())
            if len(associated_nodes[keys[0]]) < num_node_in_shard:
                    associated_nodes[keys[0]].append(n)

    # in the old node list but not the new -- need to delete from associated_nodes
    for n in old_nodelist:
        if n not in nodelist:
            old_node_need_rm.append(n)
            # remove nodes from associated_nodes
            for key in associated_nodes.keys():
                associated_nodes[key].remove(n, None)
    
    for i in range(num_shard_need_to_add):  # for each shard that we need to add
        for j in range(num_node_in_shard):  # for the min number of nodes per shard
            sorted_shards = [k for k in sorted(associated_nodes, key=lambda k: len(associated_nodes[k]))]  # sort shards by number of nodes
            max_shard_id = sorted_shards[-1]  # get shard with most nodes
            min_shard_id = sorted_shards[0]  # get shard with least nodes
            popped = associated_nodes[max_shard_id].pop()
            associated_nodes[min_shard_id].append(popped)

    for k in associated_nodes.keys():  # I think this asks a node in each shard to reshuffle?
        for n in associated_nodes[k]:
            if n == NAME:  # not 100% sure what this means
                continue
            url = f'http://{n}/update_view'
            requests.put(url, json=json.dumps(associated_nodes), timeout=1)
    
    for n in old_node_need_rm:
        url = f'http://{n}/kvs/admin/view'
        requests.delete(url, json=json.dumps(associated_nodes), timeout=1)

# def rehash_key_send_to_new_shard(data: Kvs, hashRing: HashRing, current_shard_id, associated_nodes: dict):
#     for k in data.get_all_keys():
#         shard, _= hashRing.assign(k)
#         if shard != current_shard_id:
#             for n in associated_nodes[shard]:
#                 url = f'http://{n}/reshuffle'
#                 requests.put(url, json={k: data.get(k).asDict()}, timeout=1)



