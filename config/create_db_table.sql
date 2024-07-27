-- Create database and set user
CREATE DATABASE crypto;
CREATE USER 'crypto_user' IDENTIFIED BY 'crypto_pass';
GRANT ALL PRIVILEGES ON crypto.* TO 'crypto_user';


-- Create TABLE
CREATE TABLE crypto.price (
    `dt` DATETIME,
    `BTC-USD` DECIMAL(10, 6),
    `BCH-USD` DECIMAL(10, 6),
    `ETH-USD` DECIMAL(10, 6),
    `ETC-USD` DECIMAL(10, 6),
    `XRP-USD` DECIMAL(10, 6),
    `BNB-USD` DECIMAL(10, 6),
    `LINK-USD` DECIMAL(10, 6),
    `LTC-USD` DECIMAL(10, 6),
    `EOS-USD` DECIMAL(10, 6),
    `XLM-USD` DECIMAL(10, 6),
    `ADA-USD` DECIMAL(10, 6),
    `USDT-USD` DECIMAL(10, 6)
);