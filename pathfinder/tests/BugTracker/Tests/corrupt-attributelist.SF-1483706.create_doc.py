import random

ELEMENT_LIST = ['file','folder','email','data','url']
ATTR_LIST = ['xstart','xend','name', 'size', 'xpoint']

def emitAttribute(name, value, f):
    f.write(" %s=\"%s\"" % (name, value))

def emitElementStart(name, attr, level, f):
    f.write("%s<%s" % (" "*level, name))
    for (i,j) in attr:
        emitAttribute(i, j, f)
    f.write(">\n")

def emitElementEnd(name, level, f):
    f.write("%s</%s>\n" % (" " * level, name))

id = 1

def createTree(size, level, f):
    global id
    pos = 0
    name = random.choice(ELEMENT_LIST)
    attr = [("xid",id)]
    for i in random.sample(ATTR_LIST, 2):
        attr += [(i, random.randint(0,10))]
    emitElementStart(name, attr, level, f)
    id += 1
    if size > 1:
        while pos < size:
            elemsize = random.randint(1,size/5 +1)
            createTree(elemsize, level + 1, f)
            pos += elemsize
    emitElementEnd(name, level, f)

f = open("xid.xml","w")

random.seed(1483706)

#createTree(500000, 0, f) # is shredded correctly
createTree(1200000, 0, f) # is shredded incorrectly

f.close()
