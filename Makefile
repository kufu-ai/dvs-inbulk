.PHONY: dev
dev:
	docker-compose build inbulk
	docker-compose run inbulk bash

