# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  # Client.
  config.vm.define "pc0" do |pc0|
    pc0.vm.hostname = "pc0"
    pc0.vm.network :private_network, ip: "192.168.5.2"
  end
  
  # LoadBalancer.
  config.vm.define "pc1" do |pc1|
    pc1.vm.hostname = "pc1"
    pc1.vm.network :private_network, ip: "192.168.5.3"
  end
  
  # StorageNodes.
  config.vm.define "pc2" do |pc2|
    pc2.vm.hostname = "pc2"
    pc2.vm.network :private_network, ip: "192.168.5.4"
  end
  config.vm.define "pc3" do |pc3|
    pc3.vm.hostname = "pc3"
    pc3.vm.network :private_network, ip: "192.168.5.5"
  end
  config.vm.define "pc4" do |pc4|
    pc4.vm.hostname = "pc4"
    pc4.vm.network :private_network, ip: "192.168.5.6"
  end
  config.vm.define "pc5" do |pc5|
    pc5.vm.hostname = "pc5"
    pc5.vm.network :private_network, ip: "192.168.5.7"
  end
  
  config.vm.box = "labreti/labvm-i386"
  config.ssh.forward_x11 = true
end
