import subprocess
from bashresult import BashResult
from metaexceptions import TimeOutException
import multiprocessing

class CallJava:
    def __init__(self, hivexml, timeout=0, runner=None):
        self._hivexml = hivexml
        self._timeout = timeout
        if runner:
            self._runner = runner
        else:
            self._runner = '../run.sh'

    def _calljava(self, pipe, output):
        (out, err) = pipe.communicate()
        output = BashResult(stdout=out, stderr=err)


    def calljava(self, *args):
        pipe = subprocess.Popen('{} {} {}'.format(self._runner, hivexml, ' '.join(args)), 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output = None
        process = multiprocessing.Process(target=self._calljava, args=(pipe, output))
        process.start()
        process.join(self._timeout)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeOutException("Could not complete java call in timeout={}s".format(self._timeout))

        return output
