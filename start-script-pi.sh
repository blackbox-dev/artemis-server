yes Yes | sudo apt-get install wget
yes Yes | sudo apt-get install git
yes Yes | sudo apt-get install maven

yes Yes | sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
sudo apt update
yes Yes | curl -fsSL get.docker.com -o get-docker.sh && sh get-docker.sh
sudo apt update
apt-cache policy docker-ce
yes Yes | sudo apt install docker-ce


sudo curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

#install jdk
yes Yes | sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt install openjdk-11-jdk
sudo chmod 777 /etc/environment
sudo echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/" >> /etc/environment
sudo . /etc/environment 

#download the artemis-server
git clone https://github.com/blackbox-dev/artemis-server.git
cd /artemis-server
#checkout correct branch for building docker
sudo git checkout persistToInflux 
sudo mvn clean install -Dmaven.test.skip=true
sudo chmod 777 docker/signalk/signalk_entrypoint.sh

#creates directory for telegraf config
###config has to be added manually###
sudo mkdir /etc/telegraf && sudo touch /etc/telegraf/telegraf.conf

sudo docker build --tag blackbox:signalk -f docker/signalk/Dockerfile .
sudo docker build --tag blackbox:influxdb -f docker/influxdb/Dockerfile .
sudo docker pull telegraf

sudo docker-compose up