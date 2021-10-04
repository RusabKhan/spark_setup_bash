sudo apt install default-jdk scala git -y
if spark-submit --version; then
    printf 'Spark is already installed\n'
else
    printf 'Setting up spark\n'
    wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
    tar xvf spark-*
    sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
    source ~/.profile
    printf 'Spark setup complete. Try running start-master.sh\n'
fi



