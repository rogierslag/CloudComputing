until ls /home/ubuntu/CloudComputing/.git > /dev/null 2>&1
do
  sleep 1
done

sleep 10

echo "Git checkout is done" > /tmp/start.log 2>&1;
cd /home/ubuntu/CloudComputing >> /tmp/start.log 2>&1;
git checkout monitoring >> /tmp/start.log 2>&1;
git pull >> /tmp/start.log 2>&1;
cp /home/ubuntu/CloudComputing/jgroups_discovery.xml.example /home/ubuntu/CloudComputing/jgroups_discovery.xml >> /tmp/start.log 2>&1;
cp /home/ubuntu/CloudComputing/scheduler.properties.example /home/ubuntu/CloudComputing/scheduler.properties  >> /tmp/start.log 2>&1;
sed -i 's/{{access_key}}/((access_key))/g' /home/ubuntu/CloudComputing/scheduler.properties >> /tmp/start.log 2>&1;
sed -i 's/{{secret_key}}/((secret_key))/g' /home/ubuntu/CloudComputing/scheduler.properties >> /tmp/start.log 2>&1;
sed -i 's/{{access_key}}/((access_key))/g' /home/ubuntu/CloudComputing/jgroups_discovery.xml >> /tmp/start.log 2>&1;
sed -i 's/{{secret_key}}/((secret_key))/g' /home/ubuntu/CloudComputing/jgroups_discovery.xml >> /tmp/start.log 2>&1;
sed -i 's/{{bucket}}/ping-video-cc-ewi-tudelft-nl/g' /home/ubuntu/CloudComputing/jgroups_discovery.xml >> /tmp/start.log 2>&1;

cd /home/ubuntu/CloudComputing;
mvn clean install exec:java -Dexec.mainClass="worker.Worker" -Dmaven.test.skip=true -Djgroups.bind_addr=((private_ip)) -Daws.ec2.instance-id=((instance_id)) > /tmp/cloudcomputing.log 2>&1 < /dev/null &
exit