#!/bin/zsh
_nchan_ver=0.9
_nginx_ver=1.8.0

_nchan_tag="v${_nchan_ver}"

OPTIONS=(!strip debug) #nchan is still young, in case something goes wrong we want good coredumps

if [[ ! -z $MTUNE_GENERIC ]]; then
  CFLAGS="${CFLAGS//mtune=native/mtune=generic}"
fi

_pkgname=nginx

_user="http"
_group="http"
_doc_root="/usr/share/${_pkgname}/http"
_sysconf_path="etc"
_conf_path="${_sysconf_path}/${_pkgname}"
_tmp_path="/tmp/"
_pid_path="/run"
_lock_path="/var/lock"
_access_log="/dev/stdout"
_error_log="errors.log"

pkgver() {
  echo "${_nginx_ver}.nchan${_nchan_ver}"
}


pkgname=nginx-nchan
pkgver=1.8.0.nchan0.9
pkgrel=1
pkgdesc="Nginx + Nchan - a flexible pub/sub server"
arch=('i686' 'x86_64')


depends=('pcre' 'zlib' 'openssl')
url="https://nchan.slact.net"
license=('custom')
conflicts=('nginx' 'nginx-unstable' 'nginx-svn' 'nginx-devel' 'nginx-custom' 'nginx-nchan-git') 
provides=('nginx')
backup=("${_conf_path}/conf/nginx.conf"
  "${_conf_path}/conf/koi-win"
  "${_conf_path}/conf/koi-utf"
  "${_conf_path}/conf/win-utf"
  "${_conf_path}/conf/mime.types"
  "${_conf_path}/conf/fastcgi.conf"
  "${_conf_path}/conf/fastcgi_params"
  "${_conf_path}/conf/scgi_params"
  "${_conf_path}/conf/uwsgi_params"
  "etc/logrotate.d/nginx")
_user=http
_group=http

source=("http://nginx.org/download/nginx-${_nginx_ver}.tar.gz"
  "nginx.conf"
  "nginx.logrotate"
  "nginx.service"
  "git+https://github.com/slact/nchan.git#tag=${_nchan_tag}"
       )

md5sums=('3ca4a37931e9fa301964b8ce889da8cb'
         '845cab784b50f1666bbf89d7435ac7af'
         '79031b58828462dec53a9faed9ddb36a'
         '6696dc228a567506bca3096b5197c9db'
         'SKIP')

build() {
  local _src_dir="${srcdir}/nginx-$_nginx_ver"
  local _build_dir="${_src_dir}/objs"
  cd $_src_dir
  
  CONFIGURE=(
    --prefix=/${_conf_path}
    --sbin-path=/usr/bin/nginx
    --pid-path=${_pid_path}/nginx.pid
    --lock-path=${_pid_path}/nginx.lock
    --http-client-body-temp-path=${_tmp_path}/client_body_temp
    --http-proxy-temp-path=${_tmp_path}/proxy_temp
    --http-fastcgi-temp-path=${_tmp_path}/fastcgi_temp
    --http-uwsgi-temp-path=${_tmp_path}/uwsgi_temp
    --http-scgi-temp-path=${_tmp_path}scgi_temp
    --http-log-path=${_log_path}/access.log
    --error-log-path=${_log_path}/error.log
    --user=${_user}
    --group=${_group}
    --with-http_ssl_module
    --with-http_stub_status_module
    --with-http_dav_module
    --with-http_gzip_static_module
    --with-http_realip_module
    --with-http_sub_module
    --with-http_flv_module
    --with-http_mp4_module
    --with-http_secure_link_module
    --with-debug
    --add-module="${srcdir}/nchan")

  CFLAGS="$CFLAGS -ggdb" #make sure debug symbols are present
  
  ./configure ${CONFIGURE[@]} 
  make
}

package() {
  cd "${srcdir}/nginx-${_nginx_ver}"
  make DESTDIR="$pkgdir/" install >/dev/null
  
  sed -i -e "s/\<user\s\+\w\+;/user $_user;/g" $pkgdir/$_conf_path/conf/nginx.conf
  
  mkdir -p ${pkgdir}/$_conf_path/sites-enabled/
  mkdir -p ${pkgdir}/$_conf_path/sites-available/
  
  install -d "${pkgdir}/${_tmp_path}" 
  install -d "${pkgdir}/${_doc_root}" 
  
  mv "${pkgdir}/${_conf_path}/html/"* "${pkgdir}/${_doc_root}"
  rm -rf "${pkgdir}/${_conf_path}/html"
  
  install -D -m644 "${srcdir}/nginx.logrotate" "${pkgdir}/etc/logrotate.d/${_pkgname}"
  install -D -m644 "${srcdir}/nginx.conf" "${pkgdir}/etc/conf.d/${_pkgname}"
  install -D -m644 "${srcdir}/nginx.service" "${pkgdir}/lib/systemd/system/nginx.service"
  install -D -m644 "LICENSE" "${pkgdir}/usr/share/licenses/nginx/LICENSE"

}
