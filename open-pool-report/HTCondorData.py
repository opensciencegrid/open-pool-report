'''
'''

import logging
import time
import re

import classad
import htcondor

logger = logging.getLogger(__name__)

class HTCondorData(object):
    '''
    classdocs
    '''
    schedds = []
    job_ads = []
    
    default_expressions = {
        'PeriodicRelease': ['False'], 
        'PeriodicHold': ['False'], 
        'PeriodicRemove': ['False',
                           '(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 1800)'],  # Pegasus 
        'OnExitHold': ['False',
                       '(ExitBySignal == true) || (ExitCode != 0)'],
        'OnExitRemove': ['True',
                         'NumJobCompletions > JobMaxRetries || ExitCode == 0',
                         'NumJobCompletions > JobMaxRetries || ExitCode == 0 || (ExitBySignal == false) && (ExitCode == 0)',
                         '(ExitBySignal == false) && (ExitCode == 0)',
                         '(ExitSignal is 11 || (ExitCode isnt undefined && ExitCode >= 0 && ExitCode <= 2))']
    }

    def __init__(self):
        '''
        Constructor
        '''
    
    def discover(self, usergrep = None):
        '''
        iterates over schedds and jobs to build a per-user summary data structure
        '''
        
        self._discover_schedds()
    
        for schedd in self.schedds:
            self._discover_jobs(schedd, usergrep)
        print('Found {} jobs'.format(len(self.job_ads)))
    
    
    def summarize_jobs(self):
    
        summary = {}
    
        for job in self.job_ads:
    
            # get a ref to the summary record for the user
            if job['User'] not in summary:
                summary[job['User']] = {}
                username = re.sub('@.*', '', job['User'])
                summary[job['User']]['PrintableUser'] =  '{}@{}'.format(username, job['SubmitHost'])
                summary[job['User']]['Statuses'] = {}
                summary[job['User']]['Holds'] = {}
                summary[job['User']]['PeriodicRelease'] = {}
                summary[job['User']]['PeriodicHold'] = {}
                summary[job['User']]['PeriodicRemove'] = {}
                summary[job['User']]['OnExitHold'] = {}
                summary[job['User']]['OnExitRemove'] = {}
            s = summary[job['User']]
    
            self._add_to_counter(1, s, 'TotalJobs')
            self._add_to_counter(1, s, 'Statuses', job['JobStatus'])
            
            # release/exit expressions
            for category in ['PeriodicRelease', 'PeriodicHold', 'PeriodicRemove', 'OnExitHold', 'OnExitRemove']:
                if category in job:
                    # filter out some basic ones we are not interested in
                    if not str(job[category]) in self.default_expressions[category]:
                        self._add_to_counter(1, s[category], str(job[category]))

            # hold reasons
            if 'HoldReasonCode' in job and 'HoldReasonSubCode' in job:
                codes = '[Code {} SubCode {}]'.format(job['HoldReasonCode'], job['HoldReasonSubCode'])
                reason = '{} {}'.format(job['HoldReason'], codes)
                
                # check if we already have a similar hold reason
                for old_reason, old_count in s['Holds'].items():
                    if codes in old_reason:
                        reason = old_reason
                        break

                self._add_to_counter(1, s, 'Holds', reason)

        return [summary, self.job_ads]    
    
    def _discover_schedds(self):

        print('Discovering schedds in the pool...')

        self.schedds = []

        try:
            self.schedds = htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd)
        except Exception as e:
            print('Unable to determine the scheeds: {}'.format(e))
            sys.exit(1)
        print('Found {} schedds'.format(len(self.schedds)))


    def _discover_jobs(self, schedd, usergrep):
    
        logger.info('Discovering jobs on {} ...'.format(schedd['Name']))
    
        attrs = [
                  'ClusterId',
                  'HoldReason',
                  'HoldReasonCode',
                  'HoldReasonSubCode',
                  'ImageSize',
                  'JobStatus',
                  'NumJobStarts',
                  'NumShadowStarts',
                  'PeriodicHold',
                  'PeriodicRelease',
                  'PeriodicRemove',
                  'OnExitHold',
                  'OnExitRemove',
                  'ProcId',
                  'ProjectName',
                  'SingularityImage',
                  'Requirements',
                  'RequestCpus',
                  'RequestMemory',
                  'User'
                ]
        constraint = ''
        try:
            s = htcondor.Schedd(schedd)
            jobs = s.query(constraint, attrs)
        except Exception as e:
            logger.error('Unable to query scheeds {}: {}'.format(schedd['Name'], e))
            return
    
        for j in jobs:
    
            # optionally filter the users we are want to include
            if usergrep and not re.search(usergrep,j['User']):
                continue
            
            # add submit node information
            j['SubmitHost'] = schedd['Name']
            
            self.job_ads.append(j)
            
            
    def _add_to_counter(self, amount, top, level1, level2 = None):
        if level1 not in top:
            if level2 is not None:
                top[level1] = {}
            else:
                top[level1] = 0
        if level2 is not None and level2 not in top[level1]:
            top[level1][level2] = 0
    
        # now add
        if level2 is not None:
            top[level1][level2] += amount
        else:
            top[level1] += amount
    
    

                    
                                
            
