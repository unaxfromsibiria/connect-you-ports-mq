HOME_DIR := $(HOME)
DOCKER_COMPOSE_CMD := $(shell docker help 2>/dev/null | grep -q compose && echo "docker compose" || echo "docker-compose")

example_server:
	cp example/server/docker-compose.yml docker-compose.yml
	@echo "Mqtt password option: $(shell cat /dev/urandom | tr -dc 'A-Za-z0-9' | head -c40)"
	@echo "Key option: $(shell openssl rand -hex 32)"
	@echo "Edit lines in docker-compose.yml:"
	cat docker-compose.yml | grep EDIT
	mkdir -p .mqtt_pass
	touch .mqtt_pass/password.txt && chmod 777 .mqtt_pass/password.txt
	@echo "Launch after building containers: make mqttpassword"

example_client:
	cp example/client/docker-compose.yml docker-compose.yml
	@echo "Mqtt password option: $(shell cat /dev/urandom | tr -dc 'A-Za-z0-9' | head -c40)"
	@echo "Key option: $(shell openssl rand -hex 32)"
	@echo "Edit lines in docker-compose.yml:"
	cat docker-compose.yml | grep -i EDIT

todev:
	source "$(HOME_DIR)/.cargo/env"

mqttpassword:
	@echo 'New password for user "connection" (you can edit the name)'
	$(DOCKER_COMPOSE_CMD) exec mqtt-server /opt/make_user.sh connection
