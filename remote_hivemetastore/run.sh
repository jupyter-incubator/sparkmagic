# vim: set wrap:
# vim: set nonumber:
# Run with e.g.
# . run.sh /Users/admin/altiscale_git/alti-sparkmagic/remote_hivemetastore/hive-site.xml D:all
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HERE="$DIR"
EXTRA="$DIR/java/:$DIR/java/jarlibs/*"
CP="$HERE":"$EXTRA"
java -cp $CP RemoteHiveMeta "$@"
