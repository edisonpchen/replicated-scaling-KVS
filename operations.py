
#must not be a digit or valid name char
SEP = "|"

class Operation:
    def __init__(self, name: str, key: str, n: int | str) -> None:
        self.name = name
        self.key = key
        self.n = n

    def __repr__(self) -> str:
        return f"{self.name}{SEP}{self.key}{SEP}{self.n}"

    def __eq__(self, __o: object) -> bool:
        return repr(self) == repr(__o)

    #creates an operation from its repr
    def fromString(op: str) -> "Operation":
        arr = op.split(SEP)
        name = arr[0] #first
        n = arr[-1] #last
        key = SEP.join(arr[1:-1]) #everything in the middle
        return Operation(name, key, n)

class OperationGenerator:
    def __init__(self, name: str) -> None:
        self.name = name
        self.keys: dict[int] = {}
    def nextName(self, key: str) -> Operation:
        if key in self.keys:
            op = self.keys[key]
            self.keys[key] = op + 1
            return Operation(self.name, key, op)
        self.keys[key] = 1
        return Operation(self.name, key, 0)
    
