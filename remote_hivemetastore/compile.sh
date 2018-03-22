DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CP="$DIR/java/:$DIR/java/jarlibs/*"

javac -cp $CP $DIR/java/RemoteHiveMeta.java
