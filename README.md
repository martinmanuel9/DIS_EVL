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

# Establishing Conda Environment

1. Get the following anaconda file:
   Anaconda3-2023.07-1-Linux-x86_64.sh

2. Run the following:

```bash
wget https://repo.continuum.io/archive/Anaconda3-2023.07-1-Linux-x86_64.sh
```

3. Run the following command for a x86_64:

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
conda create -n pyvenv python=3.11.4 anaconda
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
pip3 install matplotlib numpy pandas tqdm category_encoders -U scikit-learn threadpoolctl==3.1.0
```

Now everytime you begin runnig the simulations you can activate the conda environment when you login by running the following command:

```bash
conda activate pyvenv
```

# Establishing Microservices

Automate services start-up using Docker images for Cassandra, MySQL, Kafka, Zookeeper, and Spark.

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

# Running PySpark Streaming

1. **Change Directory:**
   Navigate to `DIS_EVL/models/spark-streaming`.

2. **Run Spark Setup:**
   Execute:
   ```batch
   ./spark-run.sh
   ```

Feel free to explore and experiment with the DIS_EVL environment!

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

# Running the Simulations
