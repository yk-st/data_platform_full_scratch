FROM openjdk:8

# パッケージ管理システムのアップデート
RUN apt-get -y update && apt-get -y upgrade

# localeの設定
RUN apt-get -y install locales && \
    localedef -f UTF-8 -i ja_JP ja_JP.UTF-8
ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8

# timezone (Asia/Tokyo)の設定
ENV TZ JST-9

# 必要なものインストール
RUN apt-get install -y vim postgresql-client

WORKDIR /root
