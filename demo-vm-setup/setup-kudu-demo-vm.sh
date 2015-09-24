#!/bin/bash
set -eu

# http://stackoverflow.com/questions/7126580/expand-a-possible-relative-path-in-bash
dir_resolve()
{
  cd "$1" 2>/dev/null || return $?
  echo "`pwd -P`"
}

: ${VIRTUALBOX_NAME:=cloudera-quickstart-vm-5.4.2-kudu-virtualbox}
: ${VIRTUALBOX_URL:=http://cloudera-kudu-beta.s3.amazonaws.com/${VIRTUALBOX_NAME}.ova}

# VM Settings default.
: ${VM_NAME:=kudu-demo}
: ${VM_NUM_CPUS:=2}
: ${VM_MEM_MB:=6144}

if ! which VBoxManage >/dev/null ; then
  echo "It appears that virtualbox is not installed. VBoxManage is not"
  echo "on the path. If running on Ubuntu, run apt-get -y install virtualbox"
  exit 1
fi

# Download quickstart VM
OVF=${VIRTUALBOX_NAME}.ova
if [ -e ${VIRTUALBOX_NAME}.ova ]; then
  echo Using previously downloaded image
else
  echo "Downloading Virtualbox Image file: ${VIRTUALBOX_URL}"
  curl -O ${VIRTUALBOX_URL}
fi

# Create a host only network interface
VBoxManage hostonlyif create

# Find the last one created
last_if=`VBoxManage list -l hostonlyifs | grep "^Name:" | tail -n 1 | tr " " "\n" | tail -n 1`
host_ip=`VBoxManage list -l hostonlyifs | grep "^IPAddress:" | tail -n 1 | tr " " "\n" | tail -n 1`

lower_ip=`echo $host_ip | sed 's/\([0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\)\.[0-9]\{1,3\}/\1/g'`

VBoxManage hostonlyif ipconfig $last_if --ip $host_ip
VBoxManage dhcpserver add --ifname $last_if --ip $host_ip --netmask 255.255.255.0 --lowerip $lower_ip.100 --upperip $lower_ip.200 || :
VBoxManage dhcpserver modify --ifname $last_if --enable

# Import the ovf
VBoxManage import ${OVF} --vsys 0 --cpus ${VM_NUM_CPUS} --memory ${VM_MEM_MB} --vmname ${VM_NAME} --options keepallmacs
VBoxManage modifyvm ${VM_NAME} --nic1 hostonly
VBoxManage modifyvm ${VM_NAME} --hostonlyadapter1 $last_if
VBoxManage modifyvm ${VM_NAME} --nic2 nat

# Create a shared folder with the current checkout available to the VM
REL_PATH=`pwd`/../
SHARED_FOLDER_PATH=`dir_resolve $REL_PATH`
VBoxManage sharedfolder add ${VM_NAME} --name examples --hostpath $SHARED_FOLDER_PATH --automount

# Start the VM
VBoxManage startvm ${VM_NAME}

echo "Wait until services become available."
# Wait until we can access the DFS
while true; do
    val=`VBoxManage guestproperty get $VM_NAME "/VirtualBox/GuestInfo/Net/0/V4/IP"`
    if [[ "$val" != "No value set!" ]]; then
       ip=`echo $val | awk '{ print $2 }'`
        curl http://$ip:50070/ &> /dev/null || :
         if [[ $? -eq 0 ]]; then
            break
          fi
    fi
    sleep 5
done

if ! grep -q quickstart.cloudera /etc/hosts ; then
echo "Updating the /etc/hosts file requires sudo rights."
sudo bash -e -c 'echo "#Cloudera Quickstart VM" >> /etc/hosts'
sudo bash -c "echo $ip quickstart.cloudera >> /etc/hosts"
else
echo "Hostname setup already done, check if the IP address of the VM"
echo "matches the hosts entry."
echo "IP VM: $ip"
cat /etc/hosts
fi

echo "========================================================================="
echo "Cloudera Quickstart VM installed successfully"
echo "To use the C++ and Python examples from this repository, you have to SSH"
echo "to the VM using the user 'cloudera' with the password 'cloudera'. "
echo ""
echo "You'll find the examples mounted as a shared folder at /media/sf"
