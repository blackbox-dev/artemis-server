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
yes Yes | sudo apt install apt-transport-https ca-certificates curl gnupg2 software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo apt update
apt-cache policy docker-ce
yes Yes | sudo apt install docker-ce


sudo curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

git clone https://github.com/blackbox-dev/artemis-server.git
cd /artemis-server
sudo git checkout dockerBuild

sudo docker pull davidkuba/blackbox:signalk
sudo docker pull davidkuba/blackbox:influxdb

sudo docker-compose up