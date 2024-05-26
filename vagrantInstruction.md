# Local Machine
Prior to running this on your local machine or any other machine ensure that you have the following capabilities installed:

1. Makefile and the ability to run make configurations
2. wget capabilties so that your conda environment can run
3. You have to ensure that you have virtualbox to install vagrant to run a linux based system. This will allow you to develop a VM

To ensure to run on your local machine you will need to install Vagrant using homebrew:

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/hashicorp-vagrant
```

Ensure that you have installed Vagrant:

```bash
vagrant --version
```

Once you know that vagrant and virtual box is installed run the following:

```bash
vagrant up
```

The vagrant file will run and clone the DIS_EVL repository.

By running this file you create a ubuntu virtual machine establishing docker and its containers. You have now the capability of running and adding code to this environment.

You can now SSH into the environment and virtual machine by navigating to the `.vagrant` directory. You are able to connect using VS Code IDE or any other IDE to connect to the environment and make update to the code. that has been and do the following:

1. Configure Connection: Click on "Configure SSH Hosts", then select "Add New SSH Host". Enter the SSH connection information for your Vagrant box:
   - Host: vagrant@localhost (or whatever username and IP or hostname you're using for your Vagrant box)
   - User: vagrant (or your Vagrant username)
   - Port: Default is usually 2222, but check your Vagrantfile for the SSH port number.
   - IdentityFile: Browse and select the private_key file from your .vagrant directory.

The easiest way to do this is to run the vagrant file first then get run the following commands:

```bash
cd vagrant/machine
vagrant ssh-config
```

You will then need to copy that file and paste it on your `~/.ssh` file

After you set the VS and ssh into the virtual machine you are now able to ssh into it. It is recommended to conduct the conda environment on the virtual machine so that the same configuration is complete.

**Install Java on the virtual machine**

```bash
sudo apt update
```

```bash
sudo apt install default-jre
```

```bash
sudo apt install default-jdk
```

If you have already ran the virtual machine you can run:

```bash
vagrant reload --provision
```

**The following will stop the environment**
Bring the environment down:

```bash
vagrant halt
```

**The following will completely remove the environment**
To completely remove the VM:

```bash
vagrant destroy
```

# Git commands to branch from the Main Branch

1. To update and add additional functionality for this create a new branch by running:

```bash
git switch -c <name_of_your_branch>
```

This command is able to create a new branch based on `<name_of_your_branch>` the `-c` creates a new branch

Ensure that you create from the origin/main branch by running the following:

```bash
git pull
```

This ensures that you have the latest commits of the main branch

2. You can then later switch to the main by running the following:

```bash
git switch main
```

or switching to your brand new branch from step 1

```bash
git switch <name_of_your_branch>
```

3. Assuming that you continue developing in your branch you should merge the latest updates from the main

```bash
git switch main
```

```bash
git pull
```

```bash
git switch <name_of_your_branch>
```

```bash
git pull
```

```bash
git merge --no-ff main
```

4. You will then need to determine and resolve conflicts. This can happen in the merge conflict in vs code or any other code editor