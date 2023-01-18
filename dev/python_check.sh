# check if pip3 is installed
if ! which pip3 > /dev/null; then
    # check if pyenv is installed
    export PYENV_ROOT=$1/pyenv
    if [ ! -f $PYENV_ROOT/bin/pyenv ]; then
    curl https://pyenv.run | zsh
    fi
    PATH=$PYENV_ROOT/bin:$PATH

    # check if pyenv python is installed
    PY_VERSION=`ls -1 $PYENV_ROOT/versions 2>/dev/null | tail -1`
    if [ -z $PY_VERSION ]; then
        pyenv install 3.12
        PY_VERSION=`ls -1 $PYENV_ROOT/versions 2>/dev/null | tail -1`
    fi
    PATH=$PYENV_ROOT/versions/$PY_VERSION/bin:$PATH
fi

# check if boto3 is install
if ! python3 -c 'import boto3' 2> /dev/null; then
    pip3 install boto3
fi
