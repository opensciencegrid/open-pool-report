#!/usr/bin/python3
# encoding: utf-8
'''

'''

import sys
import os

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

from pprint import pprint

from HTCondorData import HTCondorData
from EmailReport import EmailReport

__all__ = []


class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]

    try:
        # Setup argument parser
        parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]")
        parser.add_argument("-s", "--sendto", dest="sendto", action="store", help="The email address to send the report to")
        parser.add_argument("-g", "--usergrep", dest="usergrep", action="store", help="Limit results to users matching this string")
        
        # Process arguments
        args = parser.parse_args()

        verbose = args.verbose
        sendto = args.sendto
        if verbose is None:
            verbose = 0

        if verbose > 0:
            print("Verbose mode on")
            
        if sendto is None:
            sys.stderr.write("--sendto is a required argument\n")
            return 1

        htcondor = HTCondorData()
        htcondor.discover(args.usergrep)
        summary, jobs = htcondor.summarize_jobs()
    
        if verbose:
            if 'rynge@services.ci-connect.net' in summary:
                pprint(summary['rynge@services.ci-connect.net'])

        report = EmailReport(sendto, summary, jobs, verbose)
        
        # what sections do we want to include?
        report.add_summary()
        report.add_holds()
        report.add_periodic_exit_exprs()
        
        report.send()
        
        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help\n\n")
        raise
    
    

if __name__ == "__main__":
    sys.exit(main())
    
