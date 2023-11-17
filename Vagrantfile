# Vagrantfile

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/focal64"
  config.vm.network "private_network", type: "dhcp"

  config.vm.provider "virtualbox" do |vb|
    vb.memory = "2048"
    vb.cpus = 2
  end
  
  # current_directory = __dir__
  # config.vm.synced_folder current_directory, "/vagrant/DIS_EVL"

  config.vm.provision "shell", inline: <<-SHELL
    sudo apt-get update
    sudo apt-get install -y docker.io docker-compose

    sudo apt-get update
    sudo apt-get install make

    git clone https://github.com/martinmanuel9/DIS_EVL

    # Navigate to the synced directory containing the Docker Compose project
    cd DIS_EVL/models/docker

    sudo usermod -aG docker $USER
    sudo systemctl start docker
    ls -l /var/run/docker.sock
    sudo chown root:docker /var/run/docker.sock
    sudo chmod 660 /var/run/docker.sock
    sudo systemctl restart docker
    sudo make env-up

    cd ~/DIS_EVL

  SHELL
end

