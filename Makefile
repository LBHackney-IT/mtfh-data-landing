.PHONY: setup
setup:
	docker-compose build

.PHONY: build
build:
	docker-compose build mtfh-data-landing

.PHONY: serve
serve:
	docker-compose build mtfh-data-landing && docker-compose up mtfh-data-landing

.PHONY: shell
shell:
	docker-compose run mtfh-data-landing bash

.PHONY: test
test:
	docker-compose up dynamodb-database & docker-compose build mtfh-data-landing-test && docker-compose up mtfh-data-landing-test

.PHONY: lint
lint:
	-dotnet tool install -g dotnet-format
	dotnet tool update -g dotnet-format
	dotnet format

.PHONY: restart-db
restart-db:
	docker stop $$(docker ps -q --filter ancestor=dynamodb-database -a)
	-docker rm $$(docker ps -q --filter ancestor=dynamodb-database -a)
	docker rmi dynamodb-database
	docker-compose up -d dynamodb-database
