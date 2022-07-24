--ユーザーの作成
CREATE USER digdag;
--DBの作成
CREATE DATABASE digdag;
--ユーザーにDBの権限をまとめて付与
GRANT ALL PRIVILEGES ON DATABASE digdag TO digdag;
ALTER ROLE digdag WITH PASSWORD 'password';