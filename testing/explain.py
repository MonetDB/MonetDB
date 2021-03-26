
import re

def filter(tab):
    res = []
    for row in tab:
        if row[0].find('usec') < 0:
            res.append(row)
    return res

def function_histogram(tab):
    histo = {}
    for row in tab:
        if row[0].find('usec') < 0:
            g = re.match('^[^#].*\s([a-zA-Z_][a-zA-Z_0-9]*\.[a-zA-Z_][a-zA-Z_0-9]*\().*;', row[0])
            if g:
                f = g.groups(1)[0][:-1] # Remove the trailing round bracket
                if f in histo:
                    histo[f]+=1
                else:
                    histo[f]=1
    nhisto = []
    for key,val in histo.items():
        row=[]
        row.append(key)
        row.append(str(val))
        nhisto.append(row)
    return nhisto

# Returns functions returning more than one parameter in the MAL plan, at the moment, all returning parameters must be bats
def function_with_more_than_one_result_bat(tab):
    histo = {}
    for row in tab:
        if row[0].find('usec') < 0:
            g = re.match('^[^#].*\(([A-Z]\_[0-9]+:bat\[:[a-z]+\],?\ ?)+\)\ :=\ .*;', row[0])
            if g:
                g2 = re.match('^[^#].*\s([a-zA-Z_][a-zA-Z_0-9]*\.[a-zA-Z_][a-zA-Z_0-9]*\().*;', row[0])
                if g2:
                    f = g2.groups(1)[0][:-1] # Remove the trailing round bracket
                    if f in histo:
                        histo[f]+=1
                    else:
                        histo[f]=1
    nhisto = []
    for key,val in histo.items():
        row=[]
        row.append(key)
        row.append(str(val))
        nhisto.append(row)
    return nhisto
