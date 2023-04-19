import json
import sys


def usage():
    print(f"usage: {sys.argv[0]} <metadata-file>. Then stdin the val", file=sys.stderr)
    exit(1)

def getobj():
    try: 
        string=open(sys.argv[1]).read()
        obj=json.loads(string)
        return obj
    except:
        return {"causal-metadata": {}}


obj = getobj()
obj["val"] = sys.stdin.read()
print(json.dumps(obj))