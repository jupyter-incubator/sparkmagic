# vim: set wrap:
# vim: set nonumber:
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

HERE="$DIR"
EXTRA="$DIR/java/:$DIR/java/lib/*"
CP="$HERE":"$EXTRA"
java -cp $CP RemoteHiveMeta $@
