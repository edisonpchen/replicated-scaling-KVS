import unittest
import hashlib
import json



class Node:
    def __init__(self, children: list["Node"]) -> None:
        self.hash = 0
        for child in children:
            self.hash ^= child.asHash()

    def asHash(self) -> int:
        return self.hash
    def asObj(self):
        return self.hash
    def __repr__(self) -> str:
        return json.dumps(self.asObj())


class Payload(Node):
    def __init__(self, key: str, val: str) -> None:
        self.key = key
        self.val = val
        self.hash = int.from_bytes(hashlib.sha256( f"{key}:{val}".encode(),usedforsecurity=False ).digest(), 'big')
    def asHash(self) -> int:
        return self.hash
    def asObj(self):
        return {"key": self.key, "val": self.val, "hash": self.hash}
    def __repr__(self) -> str:
        return json.dumps(self.asObj())

class MerkleTree:
    def __init__(self) -> None:
        self.N = 2
        self.pyramid: list[list[Node]] = [[]]
        self.size = 0

    def __str__(self):
        return str(self.pyramid)

    def len(self):
        return self.size

    def insert(self, data: Payload):
        self.size += 1
        self.pyramid[0].append(data)
        self.rehash(len(self.pyramid[0]) - 1)

    def rehash(self, which: int):     
        for prev,next in zip(self.pyramid[0:-1], self.pyramid[1:]):
            mini = -(which % self.N)
            n = Node(prev[which + mini: which+mini+self.N])
            if which // self.N >= len(next):
                next.append(n)
            else:
                pos = which // self.N
                next[pos] = n

            which = which // self.N
        if len(self.pyramid[-1]) > 1:
            n = Node(self.pyramid[-1])
            self.pyramid.append([n])

    def findForComparison(self, row: int, unfoundNodes: list[int] | None) -> tuple[list[int], list[dict[str]]]:
        deeper = []
        info = []
        if len(self.pyramid) == 1:
            return [], [p.asObj() for p in self.pyramid[0] if not unfoundNodes or p.hash in unfoundNodes]
        if unfoundNodes is not None:
            unfoundNodes = set(unfoundNodes)
        for pos,n in enumerate(self.pyramid[-row]):
            if (row == 1 and not unfoundNodes) or n.asHash() in unfoundNodes:
                for n in self.pyramid[-(row + 1)][pos * self.N : (pos+1)*self.N]:
                    if isinstance(n, Payload):
                        info.append(n.asObj())
                    else:
                        deeper.append(n.asObj())
        return deeper, info


    def compare(self, us: set[int], other: set[int]) -> set[int]:
        return other.difference(us)
        
        
    def root(self) -> Node:
        return self.pyramid[-1][0]
        
# only finds elements you don't have that the other has, and NOT vice versa
class MerkleTreeDifferenceFinder:
    def __init__(self, merkle: MerkleTree) -> None:
        self.ourTree = merkle
        self._initSet()
        self.differences: list[dict] = []

    def _initSet(self):
        self.ourTreeSet = set()
        for row in self.ourTree.pyramid:
            for node in row:
                self.ourTreeSet.add(node.asHash())

    def dumpNextPyramidRow(self, fromRow: int, differences: list[int] | None) -> dict[str, list]:
        sendHash, sendWhole = self.ourTree.findForComparison(fromRow, differences)
        return {"nodes": sendHash, "leaves": sendWhole}

    def compareForDifferences(self, incoming: dict[str, list]) -> list[int]:
        other = incoming["nodes"]
        self.differences.extend([l for l in incoming["leaves"] if l["hash"] not in self.ourTreeSet])
        diffSet = self.ourTree.compare(self.ourTreeSet, set(other))
        return list(diffSet)

    def getResult(self) -> list:
        res = self.differences
        self.differences = []
        return res

class Tests(unittest.TestCase):
    
    def testRange(self):

        keys = [str(i) for i in range(10)]
        vals = [key*5 for key in keys]
        mt1 = MerkleTree()
        mt2 = MerkleTree()
        for (k,v) in zip(keys,vals):
            mt1.insert(Payload(k,v))
        for (k,v) in zip(reversed(keys), reversed(vals)):
            mt2.insert(Payload(k,v))
        self.assertEqual(mt1.pyramid[-1][0].asHash(), mt2.pyramid[-1][0].asHash())
    def testOne(self):
        keys = [str(i) for i in range(1)]
        vals = [key*5 for key in keys]
        mt1 = MerkleTree()
        mt2 = MerkleTree()
        for (k,v) in zip(keys,vals):
            mt1.insert(Payload(k,v))
        for (k,v) in zip(reversed(keys), reversed(vals)):
            mt2.insert(Payload(k,v))

        mf1 = MerkleTreeDifferenceFinder(mt1)
        mf2 = MerkleTreeDifferenceFinder(mt2)
        row = 1
        diff = None
        while True:
            incoming = mf1.dumpNextPyramidRow(row,diff)
            diff = mf2.compareForDifferences(incoming)
            #self.assertLessEqual(len(diff), 1)
            row += 1
            if not diff:
                break
        r=mf2.getResult()

        self.assertEqual(len(r), 0)

    def testoneDiff(self):

        m1 = MerkleTree()
        m2 = MerkleTree()
        keys = "abcdefghijklmnop"
        for l in keys:
            m1.insert(Payload(l, l*10))
            m2.insert(Payload(l, l*10))
        np = Payload("hi", "bye")
        m1.insert(np)
        
        mf1 = MerkleTreeDifferenceFinder(m1)
        mf2 = MerkleTreeDifferenceFinder(m2)

        row = 1
        diff = None
        while True:
            incoming = mf1.dumpNextPyramidRow(row,diff)
            diff = mf2.compareForDifferences(incoming)
            self.assertLessEqual(len(diff), 1)
            row += 1
            if not diff:
                break
        self.assertEqual(len(mf2.getResult()), 1)

    def testmanyDiff(self):
        m1 = MerkleTree()
        m2 = MerkleTree()
        for l in "abcdefghijklmnop":
            m1.insert(Payload(l, l*10))
            m2.insert(Payload(l, l*10))
        for l in "qrstuvwxyz":
            m1.insert(Payload(l, l*5))
        
        self.assertNotEqual(m1.root().asHash(), m2.root().asHash())
        row = 1
        diff = None
        mf1 = MerkleTreeDifferenceFinder(m1)
        mf2 = MerkleTreeDifferenceFinder(m2)
        while True:
            incoming = mf1.dumpNextPyramidRow(row,diff)
            diff = mf2.compareForDifferences(incoming)
            row += 1
            if not diff:
                break
        self.assertEqual(len(mf2.getResult()), len("qrstuvwxyz"))
    def testDisjoint(self):
        m1 = MerkleTree()
        m2 = MerkleTree()
        for l in "abcdefg":
            m1.insert(Payload(l, l*10))
        for l in "1234567":
            m2.insert(Payload(l,l*10))
        row = 1
        diff1 = None
        diff2 = None
        mf1 = MerkleTreeDifferenceFinder(m1)
        mf2 = MerkleTreeDifferenceFinder(m2)
        while True:
            incoming1 = mf1.dumpNextPyramidRow(row, diff1)
            diff1 = mf2.compareForDifferences(incoming1)
            incoming2 = mf2.dumpNextPyramidRow(row, diff2)
            diff2 = mf1.compareForDifferences(incoming2)
            row += 1
            if not diff1 and not diff2:
                break
        self.assertEqual(len(mf1.getResult()), len(mf2.getResult()))
    
      #      1
     #  #    2
    ##  ##   3
   ########  4
if __name__ == "__main__":

    unittest.main()
    


    
        
            