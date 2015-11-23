#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-nchan
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"

_clang="ccache clang -Qunused-arguments -fcolor-diagnostics"

#clang_memcheck="-fsanitize=address,undefined -fno-omit-frame-pointer"
clang_sanitize_memory="-use-gold-plugins -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -fsanitize-blacklist=bl.txt"
clang_sanitize_addres="-fsanitize=address,undefined -fno-omit-frame-pointer"

optimize_level=0;

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
    nopool|no-pool|nop) 
      export NO_POOL=1;;
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
    oldversion|old)
      export NGINX_OLDVERSION=1;;
    veryoldversion|veryold)
      export NGINX_VERYOLDVERSION=1;;
    slabpatch|slab)
      export NGX_SLAB_PATCH=1;;
    clang-analyzer|analyzer|scan|analyze)
      export CC="clang"
      export CLANG_ANALYZER=$MY_PATH/clang-analyzer
      mkdir $CLANG_ANALYZER 2>/dev/null
      ;;
  esac
done

export OPTIMIZE_LEVEL=$optimize_level

if [[ -z $NO_MAKE ]]; then
  
  ./gen_config_commands.rb nchan_config_commands.c
  if ! [ $? -eq 0 ]; then; 
    echo "failed generating nginx directives"; 
    exit 1
  fi
  ../src/store/redis/genlua.rb file
  if ! [ $? -eq 0 ]; then; 
    echo "failed generating redis lua scripts"; 
    exit 1
  fi
  pushd ./nginx-nchan >/dev/null
  
  if [[ $CONTINUE == 1 ]] || [[ $NO_EXTRACT_SOURCE == 1 ]]; then
    makepkg -f -e
  else
    makepkg -f
  fi
  popd >/dev/null
fi
if ! [[ -z $CLANG_ANALYZER ]]; then
  pushd $CLANG_ANALYZER >/dev/null
  latest_scan=`ls -c |head -n1`
  echo "run 'scan-view ${CLANG_ANALYZER}/${latest_scan}' for static analysis."
  scan-view $latest_scan 2>/dev/null
  popd >/dev/null
fi
