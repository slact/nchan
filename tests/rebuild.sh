#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-nhpm
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"
pushd ./nginx-nhpm
if [[ $1 == "nopool" ]]; then
    echo "patching to disable pools (useful for valgrind)"
    NO_POOL=1 makepkg -f
else
    makepkg -f
fi
popd
