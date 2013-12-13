#!/bin/zsh
#assumes PKGBUILDy nginx located at ./nginx-nhpm
MY_PATH="`dirname \"$0\"`"
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"
pushd ./nginx-nhpm
makepkg -f
ln -svf `pwd`/pkg/nginx-nhpm-dev/usr/bin/nginx $MY_PATH/nginx
popd
