#!/usr/bin/python3
import sys, getopt
from fritzconnection.lib.fritzhosts import FritzHosts

def main(argv):
	opts, args = getopt.getopt(argv,"m:")
	for opt, arg in opts:
		if opt == '-m':
			fh = FritzHosts(address='192.168.178.1', password='1234')
			state = fh.get_host_status(arg)
			if (state is None or state is False):
				state = 'False'
			if (state is True):
				state = 'True'
			print(arg + ';' + state)

if __name__ == "__main__":
   main(sys.argv[1:])
