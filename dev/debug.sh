#!/bin/zsh
TESTDIR=`pwd`
SRCDIR=$(readlink -m $TESTDIR/../src)
if [[ "$1" = <-> ]]; then
    sudo kdbg -p $1 $SRCDIR/nginx
else
    kdbg $SRCDIR/nginx $1
fi

