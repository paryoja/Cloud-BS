[Go to thw Cloud-BS website](https://paryoja.github.io/MRAlign/)

# About Cloud-BS

Cloud-BS is an efficient Bisulfite Sequencing alignment tool employing Apache Hadoop framework to utilize distributed parallel processing. 

# Installation
Cloud-BS is implemented on Apach Hadoop framework and utilize HDFS file system. For alignment we use Bowtie2, so that framework and programs should be prepared before executing Cloud-BS. 

# Cloud-BS on Amazon cloud
Now on Amazon EC2, AMI image is available on region Ohio. Follow under this description step-by-step to deploy Cloud-BS by using AMI image.

1. Select the route 53 service and select the hosted zones

2. Create hosted zone with name **"mralign.internal."**, private hosted zone and vpc id of the us-east-2 (Ohio) region (vpc id depends on the user). Take a note of vpc id

3. Select the ec2 service and select AMIs.

4. Select public images and search ami-a42915c1.

5. Launch ami-a42915c1 with instances having more than 16GB memory. **"Select Next: Configure instance Details"**

6. Enter the number of instances that you want to launch. Make sure that the vpc id is the same as that you entered in the route 53 service.

7. Select **Next: Add Storage** Add an EBS storage with device name /dev/sdb and size more than 20 GB.

8. Select **Next: Add Tags**

9. Select **Next: Configure Security Group**

10. Add TCP Rules with port range 4000-10000 and 50000 - 50100 from source 0.0.0.0/0.

11. Select **review and launch.** Launch the instances.

12. Select one instance as the master node and take note of private ip of the instance. For instance, 172.31.11.224.

13. Go back to the route 53 service and select hosted zones. Select **mralign.internal**. Create record set with name “master" and value “172.31.11.224”.

14. Connect to the master instance with its public ip and execute the start.sh and then connect to the other instances and execute the start.sh. You can ignore the warning “sudo: unable to resolve host ip-172-31-10-26”.

15. By using your web browser, connect “<public ip of the master node>:9870” and check every node is live. 
Then, open “<public ip of the master node>:8088” and check all nodes are live.

