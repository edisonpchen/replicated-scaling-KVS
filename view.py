from flask import Flask, request, json
import os
import requests
PORT = 8080
socket = os.environ.get('ADDRESS')

app = Flask(__name__)

nodes = []
view = []

@app.route('/kvs/admin/view', methods= ['PUT'])

def putview():

    myjson = request.json
    if (len(nodes) != 0):
        for x in range(len(nodes)):
            node = list(nodes[x].keys())[0]
            if (node not in myjson['view']): #Check for nodes not in the new view
                nodes[x][node] = 0 #Set them to uninitialized
                url = 'http://{}/kvs/admin/view'.format(node)
                response = requests.delete(url, json = node, timeout = 10)
                if (response == None):
                    return "bad", 400
        addresses = []
        for x in range(len(nodes)):
            addresses.append(list(nodes[x].keys())[0])
        for x in myjson['view']:
            if x not in addresses:
                nodes.append({x:1})
        view.clear()
        for x in myjson['view']:
            node = {x:1}
            view.append(node)

        return "populated", 200
    else:
        for x in myjson['view']:
            node = {x:1}
            view.append(node)
            nodes.append(node)
        return "empty", 200

@app.route('/kvs/admin/view', methods=['GET'])

def getview():
    mylist = []
    for x in view:
        mylist.append(list(x.keys())[0])
    return({'view': mylist}), 200

@app.route('/kvs/admin/view', methods= ['DELETE'])

def delete_node():
    node = request.json
    #Need to delete all data associated with this node
    return node

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)