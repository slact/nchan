#!/bin/zsh

nchan_root=~/sandbox/nchan

NGINX_VER=`nginx -v 2>&1 | sed "s/nginx version:\snginx\/\+//g"`
echo "building for nginx version $NGINX_VER"

echo -ne "nchan version:"
read NCHAN_VER

pushd ~/rpmbuild/SOURCES/

_nginx_src="nginx-${NGINX_VER}.tar.gz"
if [[ ! -e $_nginx_src ]]; then
    wget "https://nginx.org/download/${_nginx_src}"
    wget "https://nginx.org/download/${_nginx_src}.asc"
fi

if [[ $NCHAN_VER == "master" ]]; then
    _nchan_tag="${NCHAN_VER}"
else
    _nchan_tag="v${NCHAN_VER}"
fi

if [[ ! -e ${_nchan_tag}.tar.gz ]]; then
    wget "https://github.com/slact/nchan/archive/${_nchan_tag}.tar.gz"
fi

if [[ ! -e nchan_no_HTTP_HEADERS.patch ]]; then
    cp "$nchan_root/dev/package/fedora/nchan_no_HTTP_HEADERS.patch" ./
fi

if [[ ! -e nginx-auto-cc-gcc.patch ]]; then
    cp "$nchan_root/dev/package/fedora/nginx-auto-cc-gcc.patch" ./
fi

popd


rm ~/rpmbuild/SRPMS/* -Rfv
rm ~/rpmbuild/RPMS/* -Rfv

pushd $nchan_root/dev/package/fedora/

#git reset --hard HEAD
#git pull

rpmbuild --define "nchan_ver $NCHAN_VER" --define "nginx_ver $NGINX_VER" -ba nchan.spec

popd

scp ~/rpmbuild/RPMS/x86_64/nginx-mod-nchan-1*.x86_64.rpm ~/rpmbuild/SRPMS/nginx-mod-nchan-1*.src.rpm 10.0.2.2:/tmp
