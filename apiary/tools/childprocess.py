#!/usr/bin/python

import os
import sys
from multiprocessing import Process
from signal import signal, SIG_IGN, SIGINT

class ChildProcess(Process):
    def __init__(self, *args, **kwargs):
        super(ChildProcess, self).__init__(*args, **kwargs)
        self.daemon = True
    
    def run(self):
        # Make sure child processes don't catch ^C or eat input.
        os.close(0)
        signal(SIGINT, SIG_IGN)
        
        self.run_child_process()
