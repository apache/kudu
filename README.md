# Kudu examples

This repository holds example code and tutorials for Kudu.

## Setup Instructions with Cloudera Quickstart VM

The easiest way to try Kudu is to use the Cloudera Quickstart VM that comes
pre-packaged with Kudu and a special build of Impala integrating with Kudu. To
download and setup the VM. Simply execute the following command:

    curl -s https://raw.githubusercontent.com/cloudera/kudu-examples/master/demo-vm-setup/bootstrap.sh | bash

Once the setup is complete, you can ssh into the machine using

    ssh cloudera@quickstart.cloudera

and you can access this directory containing the examples in the following
location: `/media/sf_examples`.
