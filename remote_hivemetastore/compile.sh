DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CP="$DIR/java/:$DIR/java/lib/*"

javac -cp $CP $DIR/java/RemoteHiveMeta.java
