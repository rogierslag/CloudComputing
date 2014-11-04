CloudComputing
==============

To run, do the following

1. spin up an EC2 instance and copy your files there `rsync -e "ssh -i scheduler.priv" --exclude=".git" -r . ubuntu@<EC2_PRIVATE_IP>:CloudComputing`
1. Run `install_worker.sh` with `sudo bash install_worker.sh`
1. Start the scheduler with `mvn clean install exec:java -Dexec.mainClass="scheduler.Scheduler" -Dmaven.test.skip=true -Djgroups.bind_addr=<EC2_PRIVATE_IP>` (Set the correct IP there of course)
