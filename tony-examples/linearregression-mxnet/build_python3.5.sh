#!/bin/bash
# download and extract Python (using 3.5.7 here as an example)
export PYTHON_ROOT=${PWD}/Python3.5
curl -O https://www.python.org/ftp/python/3.5.7/Python-3.5.7.tgz
tar -xvf Python-3.5.7.tgz
rm Python-3.5.7.tgz

# compile into local PYTHON_ROOT
pushd Python-3.5.7
./configure --prefix="${PYTHON_ROOT}" --enable-unicode=ucs4
make
make install
popd
rm -rf Python-3.5.7

# install pip
pushd ${PYTHON_ROOT}
curl -O https://bootstrap.pypa.io/get-pip.py
bin/python3 get-pip.py
rm get-pip.py

# Note: add any extra dependencies here, e.g.
${PYTHON_ROOT}/bin/pip install mxnet
${PYTHON_ROOT}/bin/pip install sklearn
popd
