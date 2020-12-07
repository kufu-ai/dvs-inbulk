.PHONY: dev
dev:
	docker-compose run inbulk bash

.PHONY: test
test:
	docker-compose run inbulk-test
