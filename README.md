# Flexflow

This is a consulting project for Gourdian, Inc through Insight Data Engineering program. The code exposed here are for automating DAG creation using a YAML config file and distributed processing in Airflow. This allows for flexible execution without changing the underflying code base and scaling workloads to adapt to the business needs.

## Architecture
Airflow scheduler runs in the master node. There are 2 Celery workers. Redis acts as message broker in this case to pass messages from Celery queue to Celery worker queues. All metadata about the state of tasks and DAGs get stored in PostgreSQL database.
![architecture](images/architecture.png)

## Getting Started

### Prerequisites

This program is written in Python 3.7. Requires Airflow, Celery, PostgreSQL, and Redis. You can choose any DB or message broker. You will need to download Airflow with specified dependencies to all of the servers.

### Airflow

Install Airflow with dependencies.

```
pip3 install apache-airflow[celery,redis,s3,postgres,crypto,jdbc]
```
You can check the version to make sure Airflow is installed properly.
```
airflow -version
```
Set Airflow home in bash_profile.
```
export AIRFLOW_HOME=~/airflow
```
Reload the bash_profile.
```
source ~/.bash_profile
```
Initialize the default SQLite database with ```airflow initdb```. Once it’s done, it creates airflow.cfg and unitests.cfg.

### Redis and PostgreSQL
There are many online guides to installing Redis and PostgreSQL. Both are open source technologies. However for the interest of time, I used AWS ElastiCache and RDB managed services that have autoscale and auto-backup capabilities with a few clicks of a button.

## Linking All Parts

Go to Airflow home folder and edit the airflow.cfg. You will need to do this for all servers.

Specify where your dag folder is located. If you do not have one, you should create one.
```
dags_folder = /path/to/airflow/dags
```
Change executor to Celery.
```
executor = CeleryExecutor
```
Link to PostgreSQL metadata database.
```
sql_alchemy_conn = postgresql+psycopg2://username:password4@server_name:5432/airflow
```
Link to Redis.
```
broker_url = redis://server_name:6379/0
```
If you don't want to see example DAGs in the web UI, change load example to false.
```
load_examples = False
```
There are definitely many more options in the config file to tweak and try out. I highly encourage you to go beyond the basics.

## Getting Started

Once you have all the parts connected, it's time to initialize the scheduler, webserver, and workers.

On master server, start the scheduler and webserver. -D enables a daemon to run in the background. -p specifies the port number. Default is 8080 for Airflow web UI. Celery also has a UI at port 5555
```
airflow scheduler -D
airflow webserver -D
airflow flower -D # This is for monitoring Celery queues 
```
On worker servers, initialize workers with queue name. This is optional unless you want to specify which queues your tasks go to. Do this with all of workers with different names.
```
airflow worker -D -q queue_name
```
### Helpful Airflow Tips
If you are having problems initiating webserver due to gunicorn, try specifying the path to where gunicorn is installed. Below example is for Ubuntu server.
```
export PATH=$PATH:~/.local/bin
```
Restart the scheduler and webserver.
```
pkill -9 -f "airflow scheduler"
pkill -9 -f "airflow webserver"
pkill -9 -f "gunicorn"
airflow scheduler -D
airflow webserver -D
```
Wipe the metadata database and restart.
```
airflow resetdb
airflow initdb
airflow scheduler -D
```
If you are having problems starting the scheduler and webserver, try going to Airflow home and remove all files ending with .pid and .err.
```
cd $AIRFLOW_HOME
rm -rf *pid
rm -rf *err
```

# Questions?
Email me at joyyang03@gmai.com










