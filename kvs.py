import unittest

from operations import Operation, OperationGenerator


class KvsNode:
    def __init__(self, value, *, operation: Operation, msSinceEpoch: int, dependencies: list[Operation] = []) -> None:
        if dependencies:
            assert type(dependencies[0]) == Operation
        dependencies.append(operation)
        self.value = value
        self.operation = operation
        self.timestamp = msSinceEpoch
        self.dependencies = dependencies
    
    def asDict(self) -> dict:
        return {
            "value": self.value,
            "timestamp": self.timestamp,
            "dependencies": list(map(repr, self.dependencies)),
            "operation": repr(self.operation),
        }
    def fromDict(d: dict) -> "KvsNode":
        if "uninit" in d:
            return EMPTY_NODE
        return __class__(
            d["value"], 
            operation=Operation.fromString(d["operation"]),
            msSinceEpoch=d["timestamp"], 
            dependencies=[*map(Operation.fromString, d["dependencies"])]
        )
    def dependsOn(self, other: "KvsNode"):
        if other.operation in self.dependencies:
            return True
        return False
        

#only used for never seen before keys, use None as value for deleted keys!
class EmptyKvsNode(KvsNode):
    def __init__(self) -> None:
        self.uninit = True
        self.dependencies = []
    def asDict(self) -> dict:
        return {
            "uninit": True,
            "dependencies": []
        }
    def fromDict(d: dict):
        raise Exception("dont do this")
    def dependsOn(self, other: KvsNode):
        return False


EMPTY_NODE = EmptyKvsNode()

def getLargerNode(a: KvsNode, b: KvsNode) -> KvsNode:
    if a.dependsOn(b):
        return a
    if b.dependsOn(a):
        return b
    if a.timestamp > b.timestamp:
        return a
    if b.timestamp > a.timestamp:
        return b
    if a.operation.name > b.operation.name:
        return a
    if b.operation.name > a.operation.name:
        return b
    assert a.value == b.value, "logic error in code: we shouldnt be here right?"
    assert len(a.dependencies) == len(b.dependencies), "logic error in code: we shouldnt be here right?"
    return a


class Kvs:
    def __init__(self) -> None:
        self._data: dict[str, KvsNode] = {}
        self._opsSeen: set[str] = set()

    def __len__(self):
        return len(self.get_all_keys())

    def opsSeen(self) -> set[str]:
        return self._opsSeen
    #always works, returns true if key is new
    def put(self, key:str, node: KvsNode) -> bool:
        self._opsSeen.add(repr(node.operation))
        if key in self._data:
            wasDelete = self._data[key].value == None
            better = getLargerNode(node, self._data[key])
            self._data[key] = better
            return wasDelete if better.value != None else False
        self._data[key] = node
        return True
    
    def get(self, key:str) -> KvsNode | EmptyKvsNode:
        if key in self._data and self._data[key].value != None:
            return self._data[key]
        return EMPTY_NODE
    
    # return true on success
    def delete(self, key: str) -> bool:
        if key in self._data:
            self._data[key].value = None
            return True
        else:
            return False
    
    def get_all_keys(self) -> list[str]:
        l = []
        for k in list(self._data.keys()):
            if self._data[k].value != None:
                l.append(k)
        return l

class KvsTests(unittest.TestCase):
    def test(self):
        kvs = Kvs()
        OPGEN = OperationGenerator("TEST")
        xop = OPGEN.nextName("X")
        yop1 = OPGEN.nextName("Y")
        yop2 = OPGEN.nextName("Y")

        node1 = KvsNode("foo", msSinceEpoch=3, dependencies=[], operation=xop)
        node2 = KvsNode("bar", msSinceEpoch=3, dependencies=[], operation=yop1)
        node3 = KvsNode("bar2", msSinceEpoch=3, dependencies=[yop1], operation=yop2)
        kvs.put("X", node1)
        kvs.put("Y", node2)
        kvs.put("Y", node3)
        self.assertEqual(kvs.get("X").value, "foo")
        self.assertEqual(kvs.get("Y").value, "bar2")
    def test(self):
        kvs = Kvs()
        OPGEN = OperationGenerator("TEST")
        xop = OPGEN.nextName("X")
        yop1 = OPGEN.nextName("Y")
        yop2 = OPGEN.nextName("Y")

        node1 = KvsNode("foo", msSinceEpoch=3, dependencies=[], operation=xop)
        node2 = KvsNode("bar", msSinceEpoch=3, dependencies=[], operation=yop1)
        node3 = KvsNode("bar2", msSinceEpoch=3, dependencies=[], operation=yop2)
        kvs.put("X", node1)
        kvs.put("Y", node2)
        self.assertRaises(AssertionError, Kvs.put, kvs, "Y", node3)
        self.assertEqual(kvs.get("X").value, "foo")
        self.assertEqual(kvs.get("Y").value, "bar")

if __name__ == "__main__":
    unittest.main()