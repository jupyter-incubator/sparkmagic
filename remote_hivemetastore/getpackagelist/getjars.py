"""
Pre parse the output of a java -verbose:class run to get a set of used classes
"""

from __future__ import print_function
import subprocess

class GetJars:
    outfile = "alljars.tmp"
    cmd = "../verbose.sh > {}".format(outfile)
    
    @staticmethod
    def runjava():
        p = subprocess.Popen(GetJars.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        print(err)
    
    @staticmethod
    def getjars(jarlist):
        try:
            with open(jarlist) as f:
                jl = f.readlines()
        except IOError as e:
            print("{!r} not found. Generate this list by running {!r}".format(jarlist, GetJars.cmd))
        classes = []
        for cls in jl:
            j = cls.strip().split()[1].split('$')[0]
            classes.append(j)
    
        return set(classes)


if __name__ == '__main__':
    GetJars.runjava()
    for jar in GetJars.getjars('alljars.tmp'):
        print(jar)
