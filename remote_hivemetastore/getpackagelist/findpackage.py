from __future__ import print_function
import subprocess
from getjars import GetJars
from collections import defaultdict

PATHS = ["/opt/hadoop/share/hadoop/common", "/opt/hadoop/share/hadoop/mapreduce", "/opt/hadoop/share/hadoop/tools","/opt/hive/lib","/opt/hadoop/share/hadoop/common/lib"]
findcmd = 'grep {} {}/*'

def findpackages(classes):
    packages = defaultdict(list)
    for java in classes:
        for i, path in enumerate(PATHS):
            p = subprocess.Popen(findcmd.format(java, path), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            (out, err) = p.communicate()
            if out:
                packages[java] += [o.split()[2] for o in out.split('\n') if o]
            if not packages[java] and i == len(PATHS)-1:
                print('COULD NOT FIND: {!r}'.format(java))
            elif i == len(PATHS)-1:
                print('{!r} found in {!r}'.format(java, packages[java]))

    uniquepackages = set([j for l in packages.values() for j in l])

    return list(uniquepackages)
    

if __name__ == '__main__':
    GetJars.runjava()
    classes = GetJars.getjars(GetJars.outfile)
    uniquepackages = findpackages(classes)
    for pack in uniquepackages:
        print(pack)


