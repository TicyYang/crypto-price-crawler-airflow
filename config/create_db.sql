CREATE DATABASE crypto;
CREATE USER 'crypto_user' IDENTIFIED BY 'crypto_pass';
GRANT ALL PRIVILEGES ON crypto.* TO 'crypto_user';