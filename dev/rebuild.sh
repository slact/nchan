#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-nchan
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"

_clang="ccache clang -Qunused-arguments -fcolor-diagnostics"

#clang_memcheck="-fsanitize=address,undefined -fno-omit-frame-pointer"
clang_sanitize_memory="-use-gold-plugins -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -fsanitize-blacklist=bl.txt"
clang_sanitize_addres="-fsanitize=address,undefined -fno-omit-frame-pointer"

optimize_level=0;

export CONFIGURE_WITH_DEBUG=0

for opt in $*; do
  case $opt in
    clang)
      export CC=$_clang;;
    clang-sanitize|sanitize|sanitize-memory)
      export CC="CMAKE_LD=llvm-link $_clang -Xclang -cc1 $clang_sanitize_memory "
      export CLINKER=$clang
      ;;
    sanitize-address)
      export CC="$_clang $clang_sanitize_addres";;
    gcc5)
      export CC=gcc-5;;
    gcc4|gcc47|gcc4.7)
      export CC=gcc-4.7;;
    nopool|no-pool|nop) 
      export NO_POOL=1;;
    debug-pool|debugpool) 
      export NGX_DEBUG_POOL=1;;
    dynamic)
      export DYNAMIC=1;;
    re|remake)
      export REMAKE="-B"
      export CONTINUE=1;;
    c|continue|cont)
      export CONTINUE=1;;
    noextract)
      export NO_EXTRACT_SOURCE=1;;
    nomake)
      export NO_MAKE=1;;
    nodebug)
      export NO_DEBUG=1;;
    O0)
      optimize_level=0;;
    O1)
      optimize_level=1;;
    O2)
      optimize_level=2;;
    O3)
      optimize_level=3;;
    mudflap)
      export MUDFLAP=1
      export CC=gcc
      ;;
    stable|stableversion)
      export NGINX_STABLEVERSION=1;;
    legacyversion|legacy)
      export NGINX_LEGACYVERSION=1;;
    oldversion|old)
      export NGINX_OLDVERSION=1;;
    veryoldversion|veryold)
      export NGINX_VERYOLDVERSION=1;;
    slabpatch|slab)
      export NGX_SLAB_PATCH=1;;
    withdebug)
      export CONFIGURE_WITH_DEBUG=1;;
    clang-analyzer|analyzer|scan|analyze)
      export CC="clang"
      export CLANG_ANALYZER=$MY_PATH/clang-analyzer
      mkdir $CLANG_ANALYZER 2>/dev/null
      ;;
  esac
done

export NO_WITH_DEBUG=$NO_WITH_DEBUG;

_build_nginx() {

  if type "makepkg" > /dev/null; then
    if [[ $CONTINUE == 1 ]] || [[ $NO_EXTRACT_SOURCE == 1 ]]; then
      makepkg -f -e
    else
      makepkg -f
    fi
    return 0
  fi

  export NO_NGINX_USER=1
  export NO_GCC_COLOR=1
  export startdir="$(pwd)"
  export EXPLICIT_CFLAGS=1

  rm "${startdir}/pkg/" -Rf
  srcdir="${startdir}/src"

  source ./PKGBUILD

  pkgdir="${startdir}/pkg/${pkgname}"
  mkdir -p "$srcdir" "$pkgdir"

  echo $_nginx_source
  echo $_no_pool_patch_source
  
  wget --no-clobber $_nginx_source
  wget --no-clobber $_no_pool_patch_source

  if [[ -z $NO_EXTRACT_SOURCE ]]; then
    pushd src
    _nginx_src_file="${_nginx_source##*/}"
    echo $_nginx_src_file
    tar xf "../${_nginx_src_file}"
    cp "../${_no_pool_patch_source##*/}" ./
    if [[ ! -d ngx_debug_pool ]]; then
      git clone "$_ngx_debug_pool_url"
    else
      pushd ngx_debug_pool
      git pull
      popd
    fi
    popd
  fi

  build

  pushd "${srcdir}/nginx-${_nginx_ver}"
  ls -alh
  make DESTDIR="$pkgdir/" install
  popd
}


export OPTIMIZE_LEVEL=$optimize_level

if [[ -z $NO_MAKE ]]; then
  
  ./redocument.rb

  ./gen_config_commands.rb nchan_config_commands.c
  if ! [ $? -eq 0 ]; then; 
    echo "failed generating nginx directives"; 
    exit 1
  fi

  rdstore_dir=${MY_PATH}/../src/store/redis
  bundle exec hsss \
     --format whole \
     ${rdstore_dir}/scripts/*.lua > ${rdstore_dir}/redis_lua_commands.h
  if ! [ $? -eq 0 ]; then;
    echo "failed generating redis lua scripts";
    exit 1
  fi  
  pushd ./nginx-nchan >/dev/null
  
  _build_nginx
  
  popd >/dev/null
fi
if ! [[ -z $CLANG_ANALYZER ]]; then
  pushd $CLANG_ANALYZER >/dev/null
  latest_scan=`ls -c |head -n1`
  echo "run 'scan-view ${CLANG_ANALYZER}/${latest_scan}' for static analysis."
  scan-view $latest_scan 2>/dev/null
  popd >/dev/null
fi


