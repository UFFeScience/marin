# Marin
Tool based on the iRace library of the R language, using the marin approach for the Algorithm Configuration Problem for frameworks on a ETL Pipeline. This paper is based on previous works from other authors for optimizing framework parameters for ETL pipelines, using the iRace optimizer for the automatic algorithm configuration problem and implements in particular the F-Race algorithm for iterated races. An application was built with a pipeline with operations in the Spark and Cassandra frameworks, and applied in separate executions by iRace, optimizations for each framework independently and later we compared the results with Cassandra obtaining a result 63.7% faster than the optimization of only the Spark parameters.

1. Application Setup
2. Running directly
3. Runnig via iRace

# Application Setup

- Clone the repository, the application is under the irace-logs folder.:

```
    git clone https://github.com/UFFeScience/marin.git
```

- Files Directory structure:
    To run the application on the logs scenario you must follow the directories structures, but when running directly all files must be in the "None" folder.

- To run via iRace, first you have to change the home directory in targetRunner.sh line 4.

- Packages;
    - **Python 3**:
    ```
    sudo apt update
    sudo install python 3
    ```
    Verify installation:
    ```
    python3 --version
    ```
    Install findspark:
    ```
    pip install findspark
    ```

    - **R**:
    ```
    sudo apt-get update
    sudo apt-get install r-base
    ```
    Verify installation:
    ```
    R --version
    ```

    - **iRace**:
    Open R console:
    ```
    R
    install.packages("irace")
    ```
    Test the installation:
    ```
    # Load the package
    library("irace")
    # Obtain the installation path
    system.file(package = "irace")
    ```

    - **Java**:
    ```
    sudo apt install default-jre
    java -version
    ```
    It should output something like the following:
    ```
    openjdk 11.0.25 2024-10-15
    OpenJDK Runtime Environment (build 11.0.25+9-post-Ubuntu-1ubuntu122.04)
    OpenJDK 64-Bit Server VM (build 11.0.25+9-post-Ubuntu-1ubuntu122.04, mixed mode, sharing)
    ```

    - **Apache Spark**:
    Follow the instructions on [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

    - **Apache Cassandra**:
    Follow the instructions on [https://cassandra.apache.org/_/download.html](https://cassandra.apache.org/_/download.html)

    Open Cassandra prompt and run the following commands to create the keyspace and the logs_wordcount table:
    ```
    -- Create a keyspace
    CREATE KEYSPACE IF NOT EXISTS main_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

    -- Create the table
    CREATE TABLE main_keyspace.logs_wordcount (
    word text PRIMARY KEY, 
    count int   
    );
    ```
    
    

# Running directly
- To run the application directly, to test or debug proposes, you must call the main.py file on the irace-logs project home directory:
```
python3 /{$PROJECT_HOME}/irace-logs/main.py
```

# Runnig via iRace
- To find irace installation folder, open R console and type the following:
```
# Load the package
library("irace")
# Obtain the installation path
system.file(package = "irace")
```
The output should be something like the folloing:
```
/${USER_HOME}/R/x86_64-pc-linux-gnu-library/4.4/irace
```

- Call irace, and create a log file:
```
/${USER_HOME}/R/x86_64-pc-linux-gnu-library/4.4/irace/bin/irace >> irace.log &
```

- To follow the execution, run the command
```
tail -f irace.log
```