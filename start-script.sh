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


sudo curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

git clone https://github.com/blackbox-dev/artemis-server.git
cd artemis-server
git checkout dockerBuild
mvn package -Dmaven.test.skip=true

sudo docker build --tag blackbox:signalk -f docker/signalk/Dockerfile .
sudo docker build --tag blackbox:influxdb -f docker/influxdb/Dockerfile .

sudo docker-compose up
