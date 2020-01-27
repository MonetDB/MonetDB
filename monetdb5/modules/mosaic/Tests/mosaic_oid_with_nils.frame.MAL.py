import os
import sys

#Need to do this for Windows.
sys.path.append(os.environ['TSTSRCDIR'])


from run_mal_client import main
main(__file__)
