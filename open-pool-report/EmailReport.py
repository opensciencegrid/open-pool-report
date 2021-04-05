'''
'''

import cgi
import datetime
import re
import smtplib

from email.message import EmailMessage
from email.headerregistry import Address
from email.utils import make_msgid

from pprint import pprint

class EmailReport(object):
    '''
    classdocs
    '''

    def __init__(self, sendto, summary, jobs, verbose = 0):
        '''
        Constructor
        '''
        self.sendto = sendto
        self.data = summary
        self.jobs = jobs
        self.verbose = verbose
        
        self.body = '<p>This is a report summarizing potential workload issues in the OSG' \
                  + ' Open Pool. The purpose is to aide facilitators identify users and' \
                  + ' jobs which could benefit from intervention. Note that the report' \
                  + ' is filtering and summarizing summary to help highlight the important' \
                  + ' issues.</p>'


    def add_summary(self):
        self.body += '\n<h2>Summary</h2>\n'
        total = len(self.jobs)
        modules = 0
        singularity = 0
        simple = 0
        for details in self.jobs:
            if 'SingularityImage' in details:
                singularity += 1
            if 'Requirements' in details and \
               'has_modules' in str(details['Requirements']).lower():
                modules += 1
            # simple is if neither is required
            if 'SingularityImage' not in details and \
               ('Requirements' not in details or \
               'has_modules' not in str(details['Requirements']).lower()):
                simple += 1
                
        self.body += '<p>{:,} jobs in the pool</p><ul>\n'.format(total)
        if singularity > 0:
            self.body += '<li>{:,} jobs require Singularity ({}%)\n'.format(singularity, round(100 * float(singularity) / total))
        if modules > 0:
            self.body += '<li>{:,} jobs require Modules ({}%)\n'.format(modules, round(100 * float(modules) / total))
        if simple > 0:
            self.body += '<li>{:,} jobs requires neither ({}%)\n'.format(simple, round(100 * float(simple) / total))
        self.body += '</ul><p>Note that these might not add up. For example, some jobs might require' \
                   + ' both Singularity and Modules.</p>'        
        
        
    def add_periodic_exit_exprs(self):
        self.body += '\n<h2>Complex Periodic/Exit expressions</h2>\n<ul>\n'
        for user, details in self.data.items():
            
            if ('PeriodicRelease' in details and len(details['PeriodicRelease']) > 0) or \
               ('PeriodicHold' in details and len(details['PeriodicHold']) > 0) or \
               ('PeriodicRemove' in details and len(details['PeriodicRemove']) > 0) or \
               ('OnExitHold' in details and len(details['OnExitHold']) > 0) or \
               ('OnExitRemove' in details and len(details['OnExitRemove']) > 0):  
                self.body += '<li>{}'.format(self._userlink(details['PrintableUser']))
                self.body += '<ul>\n'
                
                for category in ['PeriodicRelease', 'PeriodicHold', 'PeriodicRemove', 'OnExitHold', 'OnExitRemove']:
                    for exprs, count in details[category].items():
                        esc = cgi.escape(exprs)
                        esc = re.sub(' ', '&nbsp;', esc)
                        self.body += '<li>{:,} jobs with <b>{}</b>: <tt style="color:green;">{}</tt>\n'.format(count, category, esc)
                                
                self.body += '</ul>\n'
                                
        self.body += '</ul>\n'


    def add_holds(self):
        self.body += '\n<h2>Held jobs</h2>\n<ul>\n'
        for user, details in self.data.items():
            
            idle = 0
            running = 0
            held = 0
            
            if 5 in details['Statuses']:
                held = details['Statuses'][5]
            else:
                continue
            
            if 1 in details['Statuses']:
                idle = details['Statuses'][1]
                
            if 2 in details['Statuses']:
                running = details['Statuses'][2]
                
            percent = int(round( 100 * float(held) / (idle + running + held)))
            # only include user over a certain threshold
            if (idle + running + held) < 20 or percent < 10:
                continue
            
            self.body += '<li>{}: '.format(self._userlink(details['PrintableUser']))
            self.body += '{:,} jobs held ({}%)\n'.format(held, percent)
            
            self.body += '<ul>\n'
            for reason, count in details['Holds'].items():
                        esc = cgi.escape(reason)
                        self.body += '<li>{:,} jobs with: <tt style="color:green;">{}</tt>\n'.format(count, esc)
            self.body += '</ul>\n'
            
        self.body += '</ul>\n\n'
        

    def send(self):
        
        today = datetime.date.today()
        
        msg = EmailMessage()
        msg['Subject'] = "Open Pool User Oddities Report - {}".format(today.strftime("%a %b %-d"))
        msg['From'] = Address("OSG Open Pool", "rynge", "flock.opensciencegrid.org")
        msg['To'] = "<{}>".format(self.sendto)
        
        msg.set_type('text/html')
        
        msg.set_content('This is an HTML - please let us know if you are not able to view it.')
        
        # Add the html version.  This converts the message into a multipart/alternative
        # container, with the original text message as the first part and the new html
        # message as the second part.
        msg.add_alternative("<html><head></head><body>\n{}\n</body></html>\n".format(self.body), subtype="html")
        
        if self.verbose:
            print(msg)
        
        # Send the message via local SMTP server.
        with smtplib.SMTP('localhost') as s:
            s.send_message(msg)
            
            
    def _userlink(self, user):
        username = re.sub('@.*', '', user)
        baseurl = "https://open-pool-display.opensciencegrid.org/d/1HF7QTDGz/user-jobs-details?orgId=1&refresh=5m&var-user="
        txt = '<a href="{}{}" target="_blank" style="color: #000099">{}</a>'.format(baseurl, username, user)
        return txt
    
    