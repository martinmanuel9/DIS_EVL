# DIS_EVL

# Setting up the Environment

1. **Clone the Codebase:**
   Download the code base from the [DIS_EVL repository](https://github.com/martinmanuel9/DIS_EVL).

2. **Directory Structure:**
   The code base has the following structure:

   - **data:** Contains IoT devices data, including TON_IoT_data and specific devices like fridge, garage, gps, modbus, motion light, thermostat, and weather.
   - **models:** Includes settings for microservices and machine learning models.
     - **dis:** Manages simulations and communication.
       - **devices:** Simulations for each device (e.g., fridgeSim.py).
       - **DISReceiver.py:** Proof-of-concept script for open DIS communication.
       - Kafka Producer and Kafka Consumer Python files generate open DIS PDU packets.
       - **RunSimulations.py:** Runs all simulations for streaming.

3. **Microservices Setup:**
   - Cassandra, Kafka, and MySQL directories have batch files for manual setup (obsolete).
   - Automation using the YML file under the docker capability.

# Option 1: Recommended to create python environment

1. Ensure you have the latest python version -> python 3.11
   a. You can download python 3.11.9 the following:

```bash
wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
```

b. Extract the download

```bash
tar -xf Python-3.11.9.tgz
```

c. Configure and build navigate to extracted directory

```bash
cd Python-3.11.9
```

d. Build configuration optimization flags:

```bash
./configure --enable-optimizations
```

e. Build Python using multiple processors

```bash
make -j$(nproc)
```

f. Install python3.11 in your system

```bash
./configure --prefix=$HOME/python3.11
make
make altinstall
```

g. Verify installation and add to path
probably run pwd to find the path.

```bash
export PATH="/<<path_to_python3.11>>/python3.11/bin:$PATH"
python3.11 --version
```

h. Set python3.11 as default

```bash
nano ~/.bashrc
```

i. Add the following at the end of the file

This may not work and you can skip steps i-k as you can still run python3.11 to establish environment

```bash
alias python3='/usr/local/bin/python3.11'
```

j. Update the bash file and apply the changes
k. Verify python version

```bash
python3 --version
```

2. Within the DIS_EVL directory run the following command to create your python environment

```bash
python3.11 -m venv pyvenv

```

3. Run and check python version:

```bash
source pyvenv/bin/activate
```

4. Run the following command to install all necessary packages

```bash
pip install -r requirements.txt
```

# Option 2: Conda Establishing Conda Environment

## recommeded if you already have a conda license. This may incur cost

If you happen to like conda and have an environment set up. The following is conda's set up

1. Get the following anaconda file:
   Anaconda3-2023.07-1-Linux-x86_64.sh

If you have an ARM core processor you will need to download the correct anaconda3 repo

2. Run the following:
   Ensure you have wget capabilities. You can always install homebrew to get wget

```bash
wget https://repo.continuum.io/archive/Anaconda3-2023.07-1-Linux-x86_64.sh
```

1. Run the following command for a x86_64:

```bash
bash Anaconda3-2023.07-1-Linux-x86_64.sh
```

4. Run the following command to activate conda and run it on the environment

```bash
source ~/anaconda3/bin/activate
```

5. Run the following to ensure you can run via bash

```bash
source ~/.bashrc
```

or

```bash
source ~/.zshrc
```

You may need to run the following to add your conda binaries:

```bash
export PATH="$HOME/anaconda3/bin:$PATH"

```

6. Update conda

```bash
conda update conda
```

or

```bash
conda update --all
```

7. Create a conda environment to make sure that your enviornment is ran correctly:

```bash
conda create -n pyvenv python=3.11.9 anaconda
```

8. Run the following command to activate the conda environment

```bash
conda activate pyvenv
```

- To deactivate this environment you can run

```bash
conda deactivate
```

9. Run the following to install python libraries to run simulations:

```bash
python -m pip install --upgrade pip
```

```bash
pip install matplotlib numpy pandas tqdm category_encoders -U scikit-learn threadpoolctl==3.1.0
```

```bash
pip install confluent_kafka
```

```bash
pip install pyspark
```

Now everytime you begin runnig the simulations you can activate the conda environment when you login by running the following command:

```bash
conda activate pyvenv
```

# Establishing Docker Images for Environment

Automate services start-up using Docker images for Cassandra, MySQL, Kafka, Zookeeper, and Spark.
We have enabled automation and wit

## UArizona ECE Compute Engineering Server

1. **Connect to Compute Environment:**
   Connect to `compute.engr.arizona.edu` with your UArizona netid.

2. **Clone and Run:**

   - Change directory to `DIS_EVL/models/docker`.
   - Execute:

     ```batch
     make env-up
     ```

     This starts Docker images, establishes MySQL and Cassandra databases, and sets up required tables.

3. **Shutdown Environment:**
   To bring the environment down:

   ```batch
   make env-down
   ```

   This shuts down the environment and removes all images.

4. **Ports Cleanup:**
   Remove all ports associated with the user during PySpark experiments.

   ```batch
   make clean-ports
   ```

# Running the Simulations

## Initializing the environment

If you have not ran the `make env-up` command from the `DIS_EVL/model/docker` directory you should ensure that you follow the microservices steps to ensure that you have the correct images to create a distributed simulated environment.

The microservices are ran via docker and container based orchestration. To automatically run these containers we have developed the `docker-env-evl-dis.yml` set-up and a make file that allows anyone to set up the environment on their system. Because the capability of providing a distributed system via Apache Kafka and Apache Spark the cluster development is conducted and established by creating these docker images. Having these containers allows you to replicate and deploy them in any region of the world.

The simple command to enable these microservices are as follows:

```bash
cd ~/DIS_EVL/models/docker
make env-up
```

It is important that after you bring your environment up you must run a bash file that allows you to create the databases for mysql and cassandra images:

```bash
./env.sh
```

To bring down the environment:

```bash
cd ~/DIS_EVL/models/docker
make env-down
```

It can be recommended to reset all ports by running the following after bringing down the environment. Please note that this will logout out of any remote login:

```bash
make port-reset
```

## Microservices running

You can verify which container you are running by running the following command:

```bash
cd ~/DIS_EVL/models/docker
make status
```

You shall see that kafka, zookeepr, mysql, cassandra, and pyspark containers are running with their perspective ports.

### Cassandra Container

To verify that cassandra is running you are able to run the following command:

```bash
docker exec -it cassandra cqlsh
```

Here you will be in the cassandra container running `cqlsh`. You can run the following to determine the keyspaces:

```cqlsh
DESCRIBE KEYSPACES;
```

You can then change use the nealy created `dis` keyspace:

```cqlsh
USE KEYSPACE dis;
```

You can now verify if the tables for the simulations were created:

```cqlsh
DESCRIBE TABLES;
```

### MYSQL Container

If you have not been able to create tables for mysql you may need to login into the container:

```bash
docker exec -it mysql_oaiids mysql -u root -p"secret"
```

From there you will need to create a database:

```sql
CREATE DATABASE IF NOT EXISTS dis;
```

You should then exit by running `exit`

Run the env bash file:

```bash
./env.sh
```

Re-enter the mysql container:

```bash
docker exec -it mysql_oaiids mysql -u root -p"secret"
```

Ensure you are using the `dis` database:

```sql
USE dis;
```

Inspect that all the tables were created:

```sql
SHOW TABLES;
```

Here you should see all tables from the IoT devices.

## Running the Proof-of-Concept simulation DIS

### Running Single Simulation opendis

We created a proof of concept for each device to ensure that we have created DIS packets that can be sent. This used the opendis model as well as custom aggregated packets created just for these IoT devices simulations. You are able to run these simulations in multiple methods:

1. Single simulation of a devices (fridge, weather device, garage, etc)
2. PDU packets that are sent via kafka
3. XML packets of the device information via kafka

We will first show how to send the packets using the PDU open DIS:

1. Navigate to `DIS_EVL/model/dis/devices`
2. Select any simulation i.e. fridgeSim.py. Uncommnent the `__main__` function
3. Update the `transmission= 'pdu'`
4. Run the script:

```bash
python fridgeSim.py
```

5. Change your director to `DIS_EVL/models/dis`
6. Run the `DISReceiver.py` script:

```bash
python DISReceiver.py
```

Here you are able to see a single device send messages to via open dis packets using UDP protocol.

### Running All devices Open DIS

This procedure is to run all devices and in Open DIS. This again takes and creates the simulations for all simulations.

1. Ensure that the simualtions under `DIS_EVL/models/dis/devices` have the `__main__` function commented out at the end of the file.
2. Change your directory back to `DIS_EVL/models/dis`
3. There is a script called `RunSimulations.py` to determine the arguements and options you can run:

```bash
python RunSimulations.py --help
```

You will have multiple options such as `transmission`, `mode`. The transmission option allows you to select whether you want to send PDUs or kafka transmission. The mode determines whether you want training or testing data.

4. Running Open DIS pdus:

```bash
python RunSimulations.py --transmission pdu --mode train
```

this will begin sending pdu packets
5.Run the `DISReceiver.py` script:

```bash
python DISReceiver.py
```

Here you will have DIS PDU packets received.

## Running A Distributed System (Kafka)

The intent is to have a distributed system in which we can execute and transport open dis pdu packets across a distributed system. Apache Kafka is great at ensuring we can develop a pub/sub relationship between topics. The simulations all have their assocaited topics (fridge, garage, light, thermostat, modbus, etc). Once the Zookeeper and Kafka iamges are running on the associated containers you can begin running these simulations.

### Kafka Single simulation

We will run a single IoT Device simulation by doing the following:
We will first show how to send the packets using the PDU open DIS:

1. Navigate to `DIS_EVL/model/dis/devices`
2. Select any simulation i.e. fridgeSim.py. Uncommnent the `__main__` function
3. Update the `transmission= 'kafka_pdu'`
4. Run the script:

```bash
python fridgeSim.py
```

5. Change your director to `DIS_EVL/models/dis`
   You have multiple options for the `KafkaConsumer.py` script as it takes in many topics:
   you can run the following:

```bash
python KafkaConsumer.py --help
```

You will see that you have `--transmission`,`--topic`, `--group_id`, `--bootstrap_server`. The important options include `transmission` and `topic`. This helps with debugging and determing that the kafka messages are correctly transported

6. Run the `KafkaConsumer.py ` script:

```bash
python KafkaConsumer.py --transmission kafka_pdu --topic fridge
```

## Running PySpark Streaming

1. **Change Directory:**
   Navigate to `DIS_EVL/models/spark-streaming`.

2. **Run Spark Setup:**
   Execute:
   ```batch
   ./spark-run.sh
   ```

Feel free to explore and experiment with the DIS_EVL environment!
