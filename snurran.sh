#!/bin/bash
set -e
if [ $# -ne 1 ] ; then
 echo "Error."
 echo "Usage: $0 version"
 echo "Example: $0 0.12.5"
 echo ""
 exit 1
fi

name="sparkmagic-${1}.tar.gz"
rm -f "$name"
cd ..
tar zcf "$name" sparkmagic 
mv "$name" sparkmagic
cd sparkmagic

scp "$name" glassfish@snurran.sics.se:/var/www/hops
