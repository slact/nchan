#!/bin/zsh
mydir=${0:a:h}

pkgdir=$mydir/pkgs

rm $pkgdir/* -fv >/dev/null 2>/dev/null

pushd $mydir/nginx-nchan
MTUNE_GENERIC=1 PKGDEST="$pkgdir" makepkg
popd

pushd $pkgdir
archpkg=`echo *pkg.tar.xz`

bundle exec fpm -s pacman -t tar $archpkg

bundle exec fpm -s pacman --no-auto-depends -d libpcre3 -d libssl1.0.2 -d zlib1g -t deb $archpkg
#bundle exec fpm -s pacman -t rpm $archpkg
#bundle exec fpm -s pacman -t osxpkg $archpkg

popd
