from decimal import Decimal
from re import sub

def do_round(res):
    return str(round(Decimal(res.group(0)), 3))

def filter(tab):
    res = []
    for row in tab:
        res.append([sub(r'\b([0-9]+\.[0-9]{4,})\b', do_round, row[0])])
    return sorted(res)
