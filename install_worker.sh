#!/bin/bash

sudo add-apt-repository ppa:mc3man/trusty-media -y
sudo apt-get install openjdk-7-jre-headless openjdk-7-jdk maven2 git curl zip ffmpeg -y

cd /home/ubuntu
[ -d /home/ubuntu/CloudComputing ] || sudo -u ubuntu git clone https://github.com/snorberhuis/CloudComputing.git