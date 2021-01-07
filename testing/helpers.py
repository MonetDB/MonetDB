# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

import os, sys
import re

def get_tests_from_all_file(fpath:str):
    res = []
    with open(fpath, 'r') as f:
        for l in f:
            # ignore comments
            if '#' in l:
                # get rid of comment anywhere on line
                l = l[:l.index('#')]
            r = l.strip()
            if r:
                comment=None
                if '#' in r:
                    comment=r[r.index('#'):]
                    r = r[:r.index('#')]
                    r = r.strip()
                # break cond from file name
                if '?' in r:
                    cond, test = r.split('?')
                    cond = cond.strip()
                    test = test.strip()
                else:
                    cond = None
                    test = r.strip()
                res.append((cond, test, comment))
    return res

def process_test_dir(dir_path:str, ctx={}, **kwargs):
    """
    Adds statistics and tests info to ctx
    """
    real_dir_path = os.path.realpath(dir_path)
    folder = {'realpath': real_dir_path}
    # check for single server
    if os.path.isfile(os.path.join(dir_path, 'SingleServer')):
        folder['single_server'] = True
        with open(os.path.join(dir_path, 'SingleServer'), 'r') as f:
            folder['server_options'] = f.read().split()
    allf = os.path.join(real_dir_path, 'All')
    tests = get_tests_from_all_file(allf) if os.path.isfile(allf) else []
    test_names = kwargs.get('test_names')
    if test_names:
        # ensure all test name exist in All file
        lookup = {}
        filtered = []
        for cond, tn, comment in tests:
            lookup[tn] = cond, tn, comment
        for tn in test_names:
            if tn in lookup:
                filtered.append(lookup[tn])
            else:
                raise ValueError('ERROR: {} does not appear to be valid test name. Check {}/All'.format(tn, dir_path))
        tests = filtered
    test_index = []
    test_lookup = {}
    lookup  = [
        # file extention  EXT        CALL      SERVER
        ('.py',           '.py',     'python', ''),
        ('.MAL.py',       '.MAL.py', 'python', 'MAL'),
        ('.SQL.py',       '.SQL.py', 'python', 'SQL'),
        ('.maltest',      '.maltest','maltest','MAL'),
        ('.malC',         '.malC',   'mal',    'MAL'),
        #('_s00.malC',     '.malC',   'malXs',  'MAL'),
        #('_p00.malC',     '.malC',   'malXp',  'MAL'),
        ('.test',         '.test',   'sqltest','SQL'),
        ('.sql',          '.sql',    'sql',    'SQL'),
        #('_s00.sql',      '.sql',    'sqlXs',  'SQL'),
        #('_p00.sql',      '.sql',    'sqlXp',  'SQL'),
        ('.R',            '.R',      'R',      'SQL'),
        ('.rb',           '.rb',     'ruby',   'SQL'),
        ('.php',          '.php',     'php',   'SQL'),
        ('.pl',           '.pl',     'perl',   'SQL'),
    ]
    if os.name == 'nt':
        lookup.append(('.MAL.bat', '.MAL.bat', 'other', ''))
        lookup.append(('.SQL.bat', '.SQL.bat', 'other', ''))
        lookup.append(('.bat', '.bat', 'other', ''))
    else:
        lookup.append(('.MAL.sh', '.MAL.sh', 'other', ''))
        lookup.append(('.SQL.sh', '.SQL.sh', 'other', ''))
        lookup.append(('.sh', '.sh', 'other', ''))
    for cond, test_name, comment in tests:
        test_path = os.path.join(real_dir_path, test_name)
        test = {
            'test_name': test_name,
            'test_path': test_path,
            'comment': comment,
            'cond': cond}
        # required tests that needs to run before this test
        # TODO enforce order at the end
        if os.path.isfile(test_path + '.reqtests'):
            reqtests = []
            missing_reqtests = []
            with open(test_path + '.reqtests', 'r') as f:
                for l in f:
                    reqtests.append(l.strip())
            test['reqtests'] = reqtests
            for r in reqtests:
                try:
                    test_lookup[r]['is_reqtest'] = True
                except KeyError:
                    print("WARNING: test name {} required to be declared before {}".format(r, test_name))
            #for r in reqtests:
            #    rp = os.path.join(real_dir_path, test_name)
            #    for ext, _, _, _ in lookup:
            #        if os.path.isfile(rp + ext) \
            #            or os.path.isfile(rp + ext + 'src') \
            #            or os.path.isfile(rp + ext + 'in'):
            #                break
            #    else:
            #        missing_reqtests.append(r)
            #test['missing_reqtests'] = missing_reqtests
        # required modules
        if os.path.isfile(test_path + '.modules'):
            reqmodules = []
            with open(test_path + '.modules', 'r') as f:
                for l in f:
                    line = l.strip()
                    if line[0] != '#':
                        reqmodules.append(line)
            test['reqmodules'] = reqmodules
        if os.path.isfile(test_path + '.nomito'):
            test['nomito'] = True
        if os.path.isfile(test_path + '.timeout'):
            with open(test_path + '.timeout', 'r') as f:
                test['timeout'] = int(f.read().strip())
        if os.path.isfile(test_path + '.options5'):
            with open(test_path + '.options5', 'r') as f:
                test['options5'] = f.read().split()
        for ext, _, call, server in lookup:
            if os.path.isfile(test_path + ext):
                test['ext'] = ext
                test['tail'] = ext
                test['call'] = call
                test['server'] = server
                break
            if os.path.isfile(test_path + ext + '.src'):
                test['ext'] = ext
                test['tail'] = ext + '.src'
                test['call'] = call
                test['server'] = server
                test['is_src_link'] = True
                break
            if os.path.isfile(test_path + ext + '.in'):
                test['ext'] = ext
                test['tail'] = ext + '.in'
                test['call'] = call
                test['server'] = server
                test['is_input'] = True
                break
        else:
            print("WARNING: test name {} declared but not found under {}".format(test_name, dir_path))
            continue
        test_index.append(test_name)
        test_lookup[test_name] = test
    tests_out = list(map(lambda x: test_lookup[x], test_index))
    folder['test_count'] = len(tests_out)
    folder['tests'] = tests_out
    ctx['test_folders'].append(folder)
    ctx['test_count'] += len(tests_out)
    return ctx

def process_dir(dir_path: str, ctx={}, **kwargs):
    if os.path.basename(os.path.realpath(dir_path)) == 'Tests':
        return process_test_dir(dir_path, ctx, **kwargs)
    onlydirs = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
    for d in onlydirs:
        dir_ = os.path.join(dir_path, d)
        process_dir(dir_, ctx, **kwargs)

def build_work_ctx(*args):
    """
    builds testing context
    args:
       single directory, or list of directories, or directory followed by list of tests within it
    return:
        ctx object
    """
    ctx = {'test_folders': [], 'test_count': 0}
    if len(args) == 0:
        return ctx
    if len(args) == 1 and os.path.isdir(args[0]):
        process_dir(args[0], ctx)
        return ctx
    is_all_dir = True
    for n in args:
        if not os.path.isdir(n):
            is_all_dir = False
            break
    if is_all_dir:
        for d in args:
            process_dir(d, ctx)
        return ctx
    # check for first arg being dir, following being valid test files within that directory
    dir_path = os.path.realpath(args[0]) if os.path.isdir(args[0]) else None
    if dir_path and os.path.basename(dir_path) == 'Tests':
        process_test_dir(dir_path, ctx, test_names=args[1:])
    else:
        raise ValueError('ERROR: {} is not a valid Tests directory'.format(args[0]))
    return ctx

if __name__ == '__main__':
    ctx = build_work_ctx(*sys.argv[1:])
    import json
    print(json.dumps(ctx, indent=4))
