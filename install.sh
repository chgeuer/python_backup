
sudo python2.7 -m ensurepip --default-pip
sudo python2.7 -m pip install --upgrade pip
# sudo python2.7 -m pip install azure-cli pid
sudo python2.7 -m pip install pid


sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
sudo sh -c 'echo -e "[azure-cli]\nname=Azure CLI\nbaseurl=https://packages.microsoft.com/yumrepos/azure-cli\nenabled=1\ntype=rpm-md\ngpgcheck=1\ngpgkey=https://packages.microsoft.com/keys/microsoft.asc" > /etc/zypp/repos.d/azure-cli.repo'
sudo zypper refresh
sudo zypper install --from azure-cli -y azure-cli
