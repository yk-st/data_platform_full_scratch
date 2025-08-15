FROM python:3.9-slim-bullseye

# busterリポジトリをarchiveへ、期限チェックを無効化して更新
RUN set -eux; \
  apt-get update; \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    default-mysql-client \
    ca-certificates \
    curl tar \
  && rm -rf /var/lib/apt/lists/*

COPY kafka_2.13-3.0.2.tgz /
#RUN wget https://dlcdn.apache.org/kafka/3.0.2/kafka_2.13-3.0.2.tgz
RUN tar -xzvf kafka_2.13-3.0.2.tgz

RUN ln -s kafka_2.13-3.0.2 kafka

COPY start_up.sh  /etc/rc.local/
RUN chmod 755 /etc/rc.local/start_up.sh
