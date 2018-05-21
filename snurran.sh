#!/bin/bash
set -e
if [ $# -ne 1 ] ; then
 echo "Error."
 echo "Usage: $0 version"
 echo "Example: $0 0.12.5"
 echo ""

name="sparkmagic-${1}.tar.gz"
rm -f "$name"

tar zcf "$name" sparkmagic 

scp "$name" glassfish@snurran.sics.se:/var/www/hops
