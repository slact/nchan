%global  _hardened_build     1
%global  nginx_user          nginx

# gperftools exist only on selected arches
%ifnarch s390 s390x
%global with_gperftools 1
%endif

%global with_aio 1

%if 0%{?fedora} > 22
%global with_mailcap_mimetypes 1
%endif


%if 0%{!?nchan_ver:1}
  %define nchan_ver master
%endif

%if "%{nchan_ver}" == "master"
  %define         nchan_tag %{nchan_ver}
%else
  %define         nchan_tag v%{nchan_ver}
%endif

%if 0%{!?nginx_ver:1}
  %define            nginx_ver 1.12.1
%endif

Name:              nginx-mod-nchan
Epoch:             1
Version:           %{nginx_ver}_%{nchan_ver}
Release:           1%{?dist}

Summary:           Flexible pub/sub server for the modern web.
License:           MIT
URL:               https://nchan.slact.net/

Source0:           http://nginx.org/download/nginx-%{nginx_ver}.tar.gz
Source1:           http://nginx.org/download/nginx-%{nginx_ver}.tar.gz.asc
Source110:         https://github.com/slact/nchan/archive/%{nchan_tag}.tar.gz

# removes -Werror in upstream build scripts.  -Werror conflicts with
# -D_FORTIFY_SOURCE=2 causing warnings to turn into errors.
Patch0:            nginx-auto-cc-gcc.patch
Patch1:            nchan_no_HTTP_HEADERS.patch

%if 0%{?with_gperftools}
BuildRequires:     gperftools-devel
%endif
BuildRequires:     openssl-devel
BuildRequires:     pcre-devel
BuildRequires:     zlib-devel

#stuff for modules we won't be using
BuildRequires:     gd-devel
BuildRequires:     perl-devel
%if 0%{?fedora} >= 24
BuildRequires:     perl-generators
%endif
BuildRequires:     perl(ExtUtils::Embed)
BuildRequires:     libxslt-devel

Requires:          nginx-filesystem >= %{epoch}:%{nginx_ver}-%{release}

%if 0%{?rhel} || 0%{?fedora} < 24
# Introduced at 1:1.10.0-1 to ease upgrade path. To be removed later.
Requires:          nginx-all-modules >= %{epoch}:%{nginx_ver}-%{release}
%endif

Requires:          nginx = %{epoch}:%{nginx_ver}-%{release}
Requires:          openssl
Requires:          pcre
Requires(pre):     nginx-filesystem
%if 0%{?with_mailcap_mimetypes}
Requires:          nginx-mimetypes
%endif
Provides:          webserver

BuildRequires:     systemd
Requires(post):    systemd

%description
Nchan is a scalable, flexible pub/sub server for the modern web, built as a module for the Nginx web server. Messages are published to channels with HTTP POST requests or Websocket, and subscribed also through Websocket, long-polling, EventSource (SSE), old-fashioned interval polling, and more.
https:// nchan.slact.net

%prep
%setup -n "nginx-%{nginx_ver}" -q -a 110
%patch0 -p0
%patch1 -p0 -d nchan-%{nchan_ver}


%build
# nginx does not utilize a standard configure script.  It has its own
# and the standard configure options cause the nginx configure script
# to error out.  This is is also the reason for the DESTDIR environment
# variable.
export DESTDIR=%{buildroot}
./configure \
    --prefix=%{_datadir}/nginx \
    --sbin-path=%{_sbindir}/nginx \
    --modules-path=%{_libdir}/nginx/modules \
    --conf-path=%{_sysconfdir}/nginx/nginx.conf \
    --error-log-path=%{_localstatedir}/log/nginx/error.log \
    --http-log-path=%{_localstatedir}/log/nginx/access.log \
    --http-client-body-temp-path=%{_localstatedir}/lib/nginx/tmp/client_body \
    --http-proxy-temp-path=%{_localstatedir}/lib/nginx/tmp/proxy \
    --http-fastcgi-temp-path=%{_localstatedir}/lib/nginx/tmp/fastcgi \
    --http-uwsgi-temp-path=%{_localstatedir}/lib/nginx/tmp/uwsgi \
    --http-scgi-temp-path=%{_localstatedir}/lib/nginx/tmp/scgi \
    --pid-path=/run/nginx.pid \
    --lock-path=/run/lock/subsys/nginx \
    --user=%{nginx_user} \
    --group=%{nginx_user} \
%if 0%{?with_aio}
    --with-file-aio \
%endif
    --with-ipv6 \
    --with-http_ssl_module \
    --with-http_v2_module \
    --with-http_realip_module \
    --with-http_addition_module \
    --with-http_xslt_module=dynamic \
    --with-http_image_filter_module=dynamic \
    --with-http_geoip_module=dynamic \
    --with-http_sub_module \
    --with-http_dav_module \
    --with-http_flv_module \
    --with-http_mp4_module \
    --with-http_gunzip_module \
    --with-http_gzip_static_module \
    --with-http_random_index_module \
    --with-http_secure_link_module \
    --with-http_degradation_module \
    --with-http_slice_module \
    --with-http_stub_status_module \
    --with-http_perl_module=dynamic \
    --with-http_auth_request_module \
    --with-mail=dynamic \
    --with-mail_ssl_module \
    --with-pcre \
    --with-pcre-jit \
    --with-stream=dynamic \
    --with-stream_ssl_module \
%if 0%{?with_gperftools}
    --with-google_perftools_module \
%endif
    --add-dynamic-module=./nchan-%{nchan_ver} \
    --with-debug \
    --with-cc-opt="%{optflags} $(pcre-config --cflags)" \
    --with-ld-opt="$RPM_LD_FLAGS -Wl,-E" # so the perl module finds its symbols

make %{?_smp_mflags}


%install
make install DESTDIR=%{buildroot} INSTALLDIRS=vendor

find %{buildroot} -type f -iname '*.so' -exec chmod 0755 '{}' \;

install -p -d -m 0755 %{buildroot}%{_datadir}/nginx/modules
install -p -d -m 0755 %{buildroot}%{_libdir}/nginx/modules

echo 'load_module "%{_libdir}/nginx/modules/ngx_nchan_module.so";' \
     > %{buildroot}%{_datadir}/nginx/modules/mod-nchan.conf

%post
if [ $1 -eq 1 ]; then
    /usr/bin/systemctl reload nginx.service >/dev/null 2>&1 || :
fi



%files
%{_datadir}/nginx/modules/mod-nchan.conf
%{_libdir}/nginx/modules/ngx_nchan_module.so
%exclude /etc/nginx/*
%exclude /usr/sbin/nginx
%exclude /usr/share/man/*
%exclude /usr/share/nginx/html/*
%exclude %{_libdir}/nginx/modules/ngx_http_*.so
%exclude %{_libdir}/nginx/modules/ngx_mail*.so
%exclude %{_libdir}/nginx/modules/ngx_stream*.so
%exclude %{_libdir}/perl5/*
   


