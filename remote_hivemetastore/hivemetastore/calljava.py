from os.path import dirname, abspath
import subprocess

from bashresult import BashResult
from metaexceptions import TimeOutException
import multiprocessing

rundir = dirname(dirname(abspath(__file__)))

class CallJava:
    def __init__(self, hivexml, timeout=0, runner=None):
        self._hivexml = hivexml
        self._timeout = timeout
        if runner:
            self._runner = runner
        else:
            self._runner = '{}/run.sh'.format(rundir)

    def _calljava(self, pipe, return_dict):
        (out, err) = pipe.communicate()
        return_dict["out"] = out
        return_dict["err"] = err



    def calljava(self, *args):
        pipe = subprocess.Popen('{} {} {}'.format(self._runner, self._hivexml, ' '.join(args)), 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        process = multiprocessing.Process(target=self._calljava, args=(pipe, return_dict))
        process.start()

        process.join(self._timeout)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeOutException("Could not complete java call in timeout={}s".format(self._timeout))

        output = BashResult(stdout=return_dict["out"], stderr=return_dict["err"])
        return output
