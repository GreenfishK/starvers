from typing import List, Tuple

def test_get_delta_for_inserts():
    old = [("a", "a", "a"), ("b", "b", "b")]
    new = [("a", "a", "a"), ("c", "c", "c")]

    inserts = __calculate_delta(new, old)
    assert len(inserts) == 1
    assert inserts[0] == ("c", "c", "c")

def test_get_delta_for_deletions():
    old = [("a", "a", "a"), ("b", "b", "b")]
    new = [("a", "a", "a"), ("c", "c", "c")]

    inserts = __calculate_delta(old, new)
    assert len(inserts) == 1
    assert inserts[0] == ("b", "b", "b")



def __calculate_delta(triples1: List[Tuple], triples2: List[Tuple], respect_updates: bool = True) -> List[Tuple]:
    delta = []

    for t1 in triples1:
        for t2 in triples2:
            if t1[0] == t2[0] and t1[1] == t2[1]:
                if t1[2] == t2[2]:
                    break
                else:
                    #TODO handle possible changes???
                    pass
        else:
            delta.append(t1)

    return delta