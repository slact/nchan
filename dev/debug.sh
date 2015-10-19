#!/bin/zsh
TESTDIR=`pwd`
SRCDIR=$(readlink -m $TESTDIR/../src)
ln -sf $TESTDIR/nginx $SRCDIR/nginx >/dev/null
ln -sf $TESTDIR/nginx-pushmodule/src/nginx/src/ $SRCDIR/nginx-source >/dev/null
if [[ "$1" = <-> ]]; then
    sudo kdbg -p $1 $SRCDIR/nginx
else
    kdbg $SRCDIR/nginx $1
fi
rm $SRCDIR/nginx $SRCDIR/nginx-source