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
  esac
done

pushd ./nginx-nhpm
if [[ $CONTINUE == 1 ]]; then
    makepkg -f -e
else
    makepkg -f
fi
popd
