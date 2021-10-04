sudo apt install default-jdk scala git -y
if spark-submit --version; then
    printf 'Spark is already installed\n'
else
    printf 'Setting up spark\n'
    wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
    tar xvf spark-*
    sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
    echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
    echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc
    echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bashrc
    source ~/.bashrc
    printf 'Spark setup complete. Try running start-master.sh\n'
fi

