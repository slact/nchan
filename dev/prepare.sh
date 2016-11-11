#!/bin/bash
UNAME=$(uname -a)

case $UNAME in 
  *Debian*)
    echo "it's debian"
    
    #stuff needed to build nginx + nchan
    sudo apt-get install -y libssl-dev libpcre3-dev zsh
    
    #ruby deps
    sudo apt-get install -y libxslt-dev libxml2-dev
    
    #build tools
    sudo apt-get install -y lua5.2 ruby bundler
    bundle install
    
    #convenience tools
    sudo apt-get install -y emacs-nox htop lsof strace ack-grep
    
    ;;
esac
