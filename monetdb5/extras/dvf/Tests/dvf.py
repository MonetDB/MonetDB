import sys
import os
try:
        from MonetDBtesting import process
except ImportError:
        import process

def client(input):
        c = process.client('sql',
                stdin = process.PIPE,
                stdout = process.PIPE,
                stderr = process.PIPE)
        out, err = c.communicate(input)
        sys.stdout.write(out)
        sys.stderr.write(err)

f = open(os.path.join(os.environ['TSTSRCDIR'], 'initializer.sql'), 'r')

script1 = f.read() + '''\
SELECT AVG(sample_value) FROM mseed.dataview WHERE network = 'FR';
SELECT MAX(sample_value) FROM mseed.dataview WHERE seq_no < 1000;
'''

def main():
        s = process.server(args = ["--set", "gdk_readonly=yes"],
                stdin = process.PIPE,
                stdout = process.PIPE,
                stderr = process.PIPE)
        client(script1)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

if __name__ == '__main__':
        main()
