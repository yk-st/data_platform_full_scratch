FROM openjdk:8

RUN apt-get update 

RUN apt-get install -y sudo

# RUN apt-get -y install software-properties-common \
#   && add-apt-repository ppa:openjdk-r/ppa \
#   && sudo apt-get update \
#   && sudo add-apt-repository --remove ppa:mc3man/trusty-media \
#   && sudo apt-get install openjdk-8-jdk

RUN apt-get update && apt-get install -y --no-install-recommends wget build-essential libreadline-dev \ 
libncursesw5-dev libssl-dev libsqlite3-dev libgdbm-dev libbz2-dev liblzma-dev zlib1g-dev uuid-dev libffi-dev libdb-dev

RUN apt-get update && apt-get install -y python3.9 python3.9-dev python3-distutils

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py \
    && pip install -U pip 

#任意バージョンのpython install
# RUN wget --no-check-certificate https://www.python.org/ftp/python/3.9.5/Python-3.9.5.tgz \
# && tar -xf Python-3.9.5.tgz \
# && cd Python-3.9.5 \
# && ./configure --enable-optimizations\
# && make \
# && make install

RUN echo pyspark ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/pyspark\
    && chmod 0440 /etc/sudoers.d/pyspark

RUN sudo mkdir -p /home/pyspark
RUN sudo chmod 777 -R /home/pyspark

RUN groupadd pyspark

RUN useradd -s /bin/bash pyspark -g pyspark

#RUN sudo apt install -y openjdk-11-jdk
RUN sudo apt-get install -y default-mysql-client

RUN sudo pip install findspark

RUN sudo apt install -y git vim
# 必要なものインストール
RUN sudo touch /etc/apt/sources.list.d/pgdg.list
RUN sudo echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN sudo apt install -y postgresql-client

WORKDIR /home/pyspark 

RUN wget https://dlcdn.apache.org/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz
RUN tar -xzvf spark-3.2.3-bin-hadoop3.2.tgz

RUN sudo mkdir -p /tmp/spark-events 
RUN sudo chmod 777 -R /tmp/spark-events
RUN sudo mkdir /var/log/spark/
RUN sudo chmod 777 -R /var/log/spark
RUN sudo mkdir /var/log/digdag/
RUN sudo chmod 777 -R /var/log/digdag

RUN sudo ln -s spark-3.2.3-bin-hadoop3.2 spark

# localeの設定
RUN apt-get -y install locales && \
    localedef -f UTF-8 -i ja_JP ja_JP.UTF-8
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8

# timezone (Asia/Tokyo)の設定
ENV TZ JST-9

USER pyspark
RUN echo "export SPARK_HOME=/home/pyspark/spark/" >> ~/.bashrc
RUN echo "export PATH=${PATH}:/home/pyspark/spark/bin" >> ~/.bashrc
RUN echo 'alias python="python3"' >> ~/.bashrc
RUN echo 'alias pip="pip3"' >> ~/.bashrc

RUN wget https://downloads.mysql.com/archives/get/p/3/file/mysql-connector-java-5.1.49.tar.gz
RUN tar -xzvf /home/pyspark/mysql-connector-java-5.1.49.tar.gz
RUN sudo mv /home/pyspark/mysql-connector-java-5.1.49/mysql-connector-java-5.1.49.jar /home/pyspark/spark/jars

COPY ./share/hive-site.xml /home/pyspark/spark/conf/
COPY ./share/log4j.properties /home/pyspark/spark/conf/

COPY ./hiveddls/2.3.0_utf-8.sql /home/pyspark/
COPY ./hiveddls/txn.sql /home/pyspark/

RUN wget https://dlcdn.apache.org/kafka/3.0.2/kafka_2.13-3.0.2.tgz
RUN tar -xzvf kafka_2.13-3.0.2.tgz

RUN ln -s kafka_2.13-3.0.2 kafka

RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /home/pyspark/librdkafka/
RUN cd /home/pyspark/librdkafka/ && ./configure --install-deps
RUN cd /home/pyspark/librdkafka/ && ./configure --prefix=/usr && make -j && sudo make install
RUN pip install confluent-kafka==1.7.0
RUN pip install avro==1.11.0
RUN pip install pdfminer.six==20211012
RUN pip install pandas==1.3.5
RUN pip install openpyxl==3.0.9
RUN pip install xlrd==2.0.1

WORKDIR /home/pyspark/
# RUN git clone https://github.com/yk-st/pyspark_batch
# RUN git clone https://github.com/yk-st/pyspark_datamanagement_metadata.git
# RUN git clone https://github.com/yk-st/pyspark_streaming.git
# RUN git clone https://github.com/yk-st/pyspark_super_crush_course.git
