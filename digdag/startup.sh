#!/bin/sh

# digdagをインストール(http://docs.digdag.io/getting_started.html)
curl -o ~/bin/digdag --create-dirs -L "https://dl.digdag.io/digdag-latest"
chmod +x ~/bin/digdag
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
. ~/.bashrc

# embulkをインストール
curl --create-dirs -o ~/bin/embulk -L "http://dl.embulk.org/embulk-latest.jar"
chmod +x ~/bin/embulk
# digdag でパスを通しているので以下２行は不要
#echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bash_profile
#source ~/.bash_profile

# 今回利用するPluginをインストール
embulk gem install embulk-input-postgresql
embulk gem install embulk-output-parquet

digdag server --config ./etc/digdag.properties --bind 0.0.0.0 --port 65432 --task-log /var/log/digdag/ --access-log /var/log/digdag