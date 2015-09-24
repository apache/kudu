# Cloudera Quickstart VM

## TL;DR

    curl -s https://raw.githubusercontent.com/cloudera/kudu-examples/master/demo-vm-setup/bootstrap.sh | bash

## Single Steps

To use Kudu with the special Cloudera Quickstart VM follow the below
instructions:

  * Install Oracle VirtualBox
  * `git clone https://github.com/cloudera/kudu-examples.git
  * `cd kudu-examples`
  * `./demo-vm-setup/setup-kudu-demo-vm.sh`

## VM setup

The setup script will download a VirtualBox appliance image and import it in
VirtualBox. In addition, it will create a new host-only network adapter with
DHCP. After the VM is started, it will extract the current IP address and
add a new /etc/hosts entry pointing from the IP of the VM to the hostname
"quickstart.cloudera". The reason for this entry is that HDFS and Kudu
require a working reverse name mapping. If you don't want to run the automated
steps make sure to check the individual steps in the file
`demo-vm-setup/setup-kudu-demo-vm.sh`.
