#!/bin/bash
HOST=quickstart.cloudera
USER=cloudera
ssh $USER@$HOST <<'ENDSSH'
set +x
# Install dependencies
sudo yum install -y gcc-c++ python-devel python-ipython

# Copy python client
cp -R /media/sf_examples/clients/python ~/
sudo chown cloudera:cloudera python
sudo chmod -R g+rw python
sudo chmod -R o+r python

# Install python client
sudo easy_install cython
cd python
sudo python setup.py install
ENDSSH
