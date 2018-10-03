# Flink Realtime Streaming Demos

## Project Layout

* /src
    - Java source files.
* /target
    - generated output

## Software Versions
* Java version: 8
* Maven version: 3.5.3

## Project Setup
* Clone git repo from https://github.com/vijay172/flink-pluralsight-course.git
* Ensure Java and maven is setup
* Run `mvn clean install` inside the project directory to install all project-specific node modules

## Coding Standards


## Deployment

Framework: https://flink.apache.com/

For local development, Flink uses Akka for an embedded mini Flink cluster:

Build and Deploy commands
```
mvn clean package

```
### Flink on EMR YARN setup 
(https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-flink.html)
https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/yarn_setup.html#background--internals
1. Create a keypair if you haven't already.
ssh-keygen  -t rsa -C "vijay-isg" -b 2048 -f vijay-isg -P ''

2. On AWS Console or commandline, create an EMR Cluster using advanced options to select Flink.

Sample used for creating a cluster:
aws emr create-cluster --applications Name=Hadoop Name=Flink --tags 'owner=vijay.balakrishnan@intel.com' 'project=Trueview-fd19' --ec2-attributes '{"KeyName":"vijay-isg","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0651e25e","EmrManagedSlaveSecurityGroup":"sg-cff155b4","EmrManagedMasterSecurityGroup":"sg-c0f652bb"}' --release-label emr-5.13.0 --log-uri 's3n://aws-logs-998656588450-us-west-2/elasticmapreduce/' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master - 1"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core - 2"}]' --configurations '[{"Classification":"flink-conf","Properties":{"taskmanager.numberOfTaskSlots":"2"},"Configurations":[]}]' --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 10 --service-role EMR_DefaultRole --enable-debugging --name 'Flink Cluster' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2

3. Copy all relevant jars and files to an S3 bucket:
mvn clean package  //from root directory
aws s3 cp flink-pluralsight-course-1.0.jar s3://base-testvijay-media

4. Create a config entry within .ssh/config file
Port 8157 is a free unused local port for Dynamic forwarding
Host flinknd
    HostName ec2-xx-yyy-zzz-zzzz.us-west-2.compute.amazonaws.com
	ForwardAgent yes
    User hadoop
    IdentityFile ~/.ssh/vijay-isg.pem
    ProxyCommand nc -x proxy-socks.jf.intel.com:1080 %h %p
    KeepAlive yes
	DynamicForward 8157	
	
5. ssh flinknd to master node/Job Manager
ssh flinknd

6. Copy files from S3 bucket to Master node
aws s3 cp s3://base-testvijay-media/flink-pluralsight-course-1.0.jar .

7. Submitting Flink jobs from Master node:
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

cd /usr/lib/flink

8. start YARN session 
detached session with 2 Task Manager nodes and each task manager having 8 slots/processes matching 8 vCPUs on each node
and TaskManager having 4096K or 4 MB RAM
./bin/yarn-session.sh -n 2 -s 8 -tm 4096 -d

9. 
Run Flink job on YARN session: (using S3 for input files)
cd ~

/usr/lib/flink/bin/flink run -m yarn-cluster -yn 2 -ys 8 ./flink-pluralsight-course-1.0.jar --camera camera.csv --metadata inputMetadata.csv

10. Setup FoxProxy for ssh tunnelling using resources/foxyproxy.json
Go to Firefox and setup Foxyproxy. 
Use http://localhost:8157/cluster/ to view ResourceManager UI 
or click on AWS Console -> Cluster to view -> Resource Manager link
Or Look for similar output on jobmanager.log file to find Flink DashBoard UI
org.apache.flink.runtime.webmonitor.WebRuntimeMonitor         - Web frontend listening at ip-172-31-9-104.us-west-2.compute.internal:36657

Alternative using just AWS Console:
1. Create cluster as above
2. Add step - Start YARN session (using commandrunner.jar & flink-yarn-session)
flink-yarn-session -n 2 -s 8 -d
3. Drop all jars & files onto Master node at /home/hadoop directory using scp or using S3 as an intermediary
scp -i ~/.ssh/vijay-isg.pem flink-pluralsight-course-1.0.jar  hadoop@ec2-34-219-91-229.us-west-2.compute.amazonaws.com:

4. Add step - Submit Flink Job
flink run -m yarn-cluster -yn 2 -ys 8 /home/hadoop/flink-pluralsight-course-1.0.jar --camera camera.csv --metadata inputMetadata.csv 

After the job is completed and when not streaming, you can also look at logs at for eg:
s3://aws-logs-998656588450-us-west-2/elasticmapreduce/j-3FSPYI6WUGFXL/steps/s-2YWWK2XDS5CWF/

Alternative using AWS command line only:
1. Create cluster as above.
2. Start YARN session: (configurations.json is a file under resources)
aws emr create-cluster --release-label emr-5.13.0 \
--applications Name=Flink \
--configurations file://./configurations.json \
--region us-east-1 \
--log-uri s3://myLogUri \
--instance-type m4.large \
--instance-count 2 \
--service-role EMR_DefaultRole \ 
--ec2-attributes KeyName=MyKeyName,InstanceProfile=EMR_EC2_DefaultRole \
--steps Type=CUSTOM_JAR,Jar=command-runner.jar,Name=Flink_Long_Running_Session,\
Args="flink-yarn-session -n 2 -d"
3. Submit Flink job: 
camera and inputMetadata files are pulled from s3://base-testvijay-media/ here.
Jar file is on master node itself (??)
aws emr add-steps --cluster-id j-22GU0KE9A4IGP \
--steps Type=CUSTOM_JAR,Name=Flink_Submit_To_Long_Running,Jar=command-runner.jar,\
Args="flink","run","-m","yarn-cluster","-yid","application_1526666730459_0001","-yn","2",\
"/home/hadoop/flink-pluralsight-course-1.0.jar",\
"--camera","camera.csv","--metadata","inputMetadata.csv" \
--region us-west-2 

Kill Flink client: (find applicationId from sysout log)
yarn application -kill application_1526666730459_0001

### Monitoring
See YARN logs:
yarn logs -applicationId application_1526666730459_0022

taskmanager.log.gz has standard logger output
taskmanager.out.gz has stdout output for tasks/nodes

Resource manager UI at port 8088.(port determined by yarn.resourcemanager.webapp.address)

http://ec2-34-219-91-229.us-west-2.compute.amazonaws.com/proxy/application_1526666730459_0008/

When Cluster is started, click application id link in Hadoop cluster UI at localhost:8157/cluster
look in jobmanager.log file for Job mgr IP:(search for Web Frontend)
Look for similar output on jobmanager.log file to find Flink Dashboard UI
org.apache.flink.runtime.webmonitor.WebRuntimeMonitor         - Web frontend listening at ip-172-31-9-104.us-west-2.compute.internal:36657


#### Remove an Environment

1. STOP a Yarn Session
2 ways to do it:
yarn application -kill application_1526666730459_0026

./bin/yarn-session.sh -id application_1526666730459_0001 stop

##### Snapshotting

##### Restore

## Testing

### Test Environment Variables


## AWS


### Policies

#Troubleshooting

1. Error on commandline:
open failed: connect failed: Connection refused

Make sure Foxyproxy is off. (need it only for accessing ResourceManager UI from AWS Web console and Flink Dashboard)
Then ssh back in.

#### API Semantic Versioning


## API Documentation

### Installation and Invocation

```
brew install maven

```

## Resources

* https://ci.apache.org/projects/flink/flink-docs-release-1.4/examples/
