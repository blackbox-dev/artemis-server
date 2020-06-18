sudo apt-get install wget
sudo apt-get install git
sudo apt-get install maven

sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
sudo apt update
sudo apt install apt-transport-https ca-certificates curl gnupg2 software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo apt update
apt-cache policy docker-ce
sudo apt install docker-ce


echo "deb https://packagecloud.io/Hypriot/Schatzkiste/debian/ jessie main" | sudo tee /etc/apt/sources.list.d/hypriot.list
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 37BBEE3F7AD95B3F
sudo apt-get update
sudo apt-get install docker-compose
sudo chmod +x /usr/local/bin/docker-compose

git clone https://github.com/blackbox-dev/artemis-server.git
cd artemis-server
git checkout dockerBuild
sudo chmod 777 docker/signalk/signalk_entrypoint.sh
mvn package -Dmaven.test.skip=true

sudo mkdir /etc/telegraf && sudo touch /etc/telegraf/telegraf.conf
sudo docker run --rm telegraf telegraf config | sudo tee /etc/telegraf/telegraf.conf
## replace with our config ##

sudo docker build --tag blackbox:signalk -f docker/signalk/Dockerfile .
sudo docker build --tag blackbox:influxdb -f docker/influxdb/Dockerfile .
sudo docker pull telegraf

sudo docker-compose -f docker-compose-pi.yml up
