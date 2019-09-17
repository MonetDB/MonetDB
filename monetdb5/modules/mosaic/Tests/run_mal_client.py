import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def prepare_mal_script(generic_script_name, compression_type_id):
    with open(generic_script_name, 'r') as file:
        return file.read().format(compression_type=compression_type_id)

def get_mal_script(fqn_calling_script_file):
    test_file = os.path.basename(fqn_calling_script_file)
    parts = test_file.split(".", 3)
    dir_path = os.path.dirname(os.path.realpath(fqn_calling_script_file))
    test_file = os.path.join(dir_path, parts[0] + ".mal")
    compression_type = parts[1]
    return test_file, compression_type

def client(query):
    clt = process.client('mal', stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate(input = query)

"""
The script's name <fqn_calling_script_file> is vital
to correctly call what is essential a templated MAL script.
Script name must be of the form
    <name of mal script without '.mal' extension>.<compression_technique>.MAL.py

This little python function main will look for a template MAL script called
    <name of mal script without '.mal' extension>.mal

The function main makes sure that the template parameters in this script are
replaced by the compression technique specified by <compression_technique>.
"""
def main(fqn_calling_script_file):

    test_file, compression_type = get_mal_script(fqn_calling_script_file)
    script = prepare_mal_script(test_file, compression_type)

    out, err = client(script)
    sys.stdout.write(out)
    sys.stderr.write(err)
