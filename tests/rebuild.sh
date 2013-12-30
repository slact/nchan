#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-nhpm
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
    mudflap)
      export MUDFLAP=1
      export CC=gcc
      ;;
    clang-analyzer|analyzer|scan|analyze)
      export CLANG_ANALYZER=`pwd`/clang-analyzer/ 
      echo $CLANG_ANALYZER
      mkdir $CLANG_ANALYZER 2>/dev/null
      ;;
  esac
done

pushd ./nginx-nhpm
if [[ $CONTINUE == 1 ]]; then
    makepkg -f -e
else
    makepkg -f
fi
popd

if ! [[ -z $CLANG_ANALYZER ]]; then
  pushd $CLANG_ANALYZER >/dev/null
  scan-view `ls -c |head -n1`
  popd >/dev/null
fi
