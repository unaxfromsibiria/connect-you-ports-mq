#!/bin/bash

rm -f cyp_main.zip
wget -O cyp_main.zip https://github.com/unaxfromsibiria/connect-you-ports-mq/archive/refs/heads/main.zip

if [ -f "./connect-you-ports-mq-main/docker-compose.yml" ]; then
    cp "./connect-you-ports-mq-main/docker-compose.yml" "docker-compose.yml.bak"
fi

TEMP_DIR=".update_tmp_$$"
rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR"
unzip -o cyp_main.zip -d "$TEMP_DIR"
cp -r "$TEMP_DIR/connect-you-ports-mq-main/." ./connect-you-ports-mq-main
echo 'version:'
cat "$TEMP_DIR/connect-you-ports-mq-main/Cargo.toml" | grep '^version' | cut -d '=' -f2

if [ -f "docker-compose.yml.bak" ]; then
    cp "docker-compose.yml.bak" "./connect-you-ports-mq-main/docker-compose.yml"
    rm "docker-compose.yml.bak"
fi

rm -rf "$TEMP_DIR"
cd connect-you-ports-mq-main
