import shutil
from findpackage import findpackages
from getjars import GetJars

GetJars.runjava()
classes = GetJars.getjars(GetJars.outfile)
uniquepackages = findpackages(classes)

for jar in uniquepackages:
    shutil.copy(jar, '../lib')
