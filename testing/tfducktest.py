#!/usr/bin/env python3

import sys

def main():
    buff = []
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        sline = line.strip()
        if sline.startswith('statement') or sline.startswith('query'):
            words = sline.split()
            if len(words) == 3:
               third = words[2]
               if third.lower() not in ['rowsort', 'valuesort', 'nosort']:
                   # must be connection str
                   buff.append(f'@connection(id={third})\n')
                   #strip last word
                   buff.append(' '.join(words[:2]) + '\n')
                   continue
        buff.append(line)
    print(''.join(buff))

if __name__ == '__main__':
    main()

