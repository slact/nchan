#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-pushmodule
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"

for opt in $*; do
  case $opt in
    clang)
      export CC=clang;;
    nopool|no-pool|nop) 
      export NO_POOL=1;;
    c|continue|cont)
      export CONTINUE=1;;
    nomake)
      export NO_MAKE=1;;
    nodebug)
      export NO_DEBUG=1;;
    mudflap)
      export MUDFLAP=1
      export CC=gcc
      ;;
    clang-analyzer|analyzer|scan|analyze)
      export CLANG_ANALYZER=$MY_PATH/clang-analyzer
      mkdir $CLANG_ANALYZER 2>/dev/null
      ;;
  esac
done

if [[ -z $NO_MAKE ]]; then
  pushd ./nginx-pushmodule >/dev/null
  if [[ $CONTINUE == 1 ]]; then
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
