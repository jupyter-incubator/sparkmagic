import datetime
from subprocess import Popen, PIPE
from threading import Thread

import sparkmagic.utils.constants as constants


class KerberosThread(Thread):
    def __init__(self, event, endpoint):
        Thread.__init__(self)
        self.stopped = event
        self.setDaemon(True)
        self.endpoint = endpoint

    def run(self):
        self.kinit()
        while not self.stopped.wait(constants.KERBEROS_TIME_INTERVAL):
            self.kinit()

    def kinit(self):
        kinit_args = [constants.KERBEROS_KINIT, self.endpoint.username]
        kinit = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        kinit.stdin.write(('%s\n' % self.endpoint.password).encode())
        kinit.stdin.flush()
        kinit.wait()
        kinit.terminate()
