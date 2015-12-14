#!/bin/zsh
mydir=${0:a:h}

pkgdir=$mydir/pkgs

rm $pkgdir/* -fv >/dev/null 2>/dev/null

pushd $mydir/nginx-nchan-static
MTUNE_GENERIC=1 PKGDEST="$pkgdir" makepkg
popd

pushd $pkgdir
archpkg=`echo *pkg.tar.xz`

tarname=${archpkg:r}
tarname=${tarname:r}
tarname=${tarname:r}

echo $tarname

bundle exec fpm -s pacman -t tar -n "$tarname" $archpkg
gzip -f "$tarname.tar"

bundle exec fpm -s pacman --no-auto-depends -t deb $archpkg
#bundle exec fpm -s pacman -t rpm $archpkg
#bundle exec fpm -s pacman -t osxpkg $archpkg

popd
