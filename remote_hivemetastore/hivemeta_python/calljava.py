import subprocess
from bashresult import BashResult

class CallJava:
    def __init__(self, runner=None):
        if runner:
            self._runner = runner
        else:
            self._runner = '../run.sh'

    def calljava(self, *args):
        pipe = subprocess.Popen('{} {}'.format(self._runner, ' '.join(args)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = pipe.communicate()
        print err
        return BashResult(stdout=out, stderr=err)
