# Define the default target (first target in the file)
.DEFAULT_GOAL := help

# List all Docker Compose YAML files in the current directory
COMPOSE_FILES := $(wildcard *.yml)

# Define targets and their actions
help:
	@echo "Available targets:"
	@echo "  up       		- Start all services"
	@echo "  down     		- Stop and remove all containers"
	@echo "  logs     		- View container logs"
	@echo "  status   		- Show container status"
	@echo "  kafka	  		- Start Kafka Service"
	@echo "  pyspark  		- Start Pyspark Service"
	@echo "  cassandra		- Start Cassandra Service"
	@echo "  dbt			- Start dbt Service"
	@echo "  kafka-down		- Stop Kafka Service"
	@echo "  pyspark-down	- Stop Pyspark Service"
	@echo "  cassandra-down	- Stop Cassandra Service"
	@echo "  dbt-down     	- Stop this dbt Service"
	@echo "  help     		- Show this help message"


up:
	@for file in $(COMPOSE_FILES); do \
		echo "Starting services in $$file"; \
		docker-compose -f $$file up -d; \
	done

down:
	@for file in $(COMPOSE_FILES); do \
		echo "Stopping and removing containers in $$file"; \
		docker-compose -f $$file down; \
	done

logs:
	@for file in $(COMPOSE_FILES); do \
		echo "Viewing logs for containers in $$file"; \
		docker-compose -f $$file logs -f; \
	done

status:
	@for file in $(COMPOSE_FILES); do \
		echo "Container status in $$file"; \
		docker-compose -f $$file ps; \
	done

kafka:
	echo "Starting Kafka Service"
	docker-compose -f docker-compose-kafka.yml up -d
	done

pyspark:
	echo "Starting Pyspark Service"; \
	docker-compose -f docker-compose-pyspark.yml up -d
	done

cassandra:
	echo "Starting Cassandra Service"; \
	docker-compose -f docker-compose-cassandra.yml up -d
	done

dbt:
	echo "Starting dbt Service"; \
	docker-compose -f docker-compose-dbt.yml up -d
	done

kafka-down:
	echo "Stopping Kafka Service"; \
	docker-compose -f docker-compose-kafka.yml down
	done

pyspark-down:
	echo "Stopping Pyspark Service"; \
	docker-compose -f docker-compose-pyspark.yml down
	done

cassandra-down:
	echo "Stopping Cassandra Service"; \
	docker-compose -f docker-compose-cassandra.yml down
	done

dbt-down:
	echo "Stopping dbt Service"; \
	docker-compose -f docker-compose-dbt.yml down
	done
