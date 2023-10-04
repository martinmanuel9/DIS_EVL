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
	@echo "  kafka-down		- Stop Kafka Service"
	@echo "  pyspark-down	- Stop Pyspark Service"
	@echo "  cassandra-down	- Stop Cassandra Service"
	@echo "  help     		- Show this help message"


up:
	@for file in $(COMPOSE_FILES); do \
		echo "Starting services in $$file"; \
		docker-compose -f $$file up -d; \
	done

down:
	@for file in $(COMPOSE_FILES); do \
		echo "Stopping and removing containers in $$file"; \
		docker-compose -f $$file down --remove-orphans; \
	done

logs:
	@for file in $(COMPOSE_FILES); do \
		echo "Viewing logs for containers in $$file"; \
		docker-compose -f $$file logs -f; \
	done

status:
	@echo "-----------------------------------------------------------------------------------------------------------------------------------------"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}"
	@echo "-----------------------------------------------------------------------------------------------------------------------------------------"
	done

kafka:
	echo "Starting Kafka Service"
	docker-compose -f docker-compose-kafka.yml up -d;
	done

pyspark:
	echo "Starting Pyspark Service"; \
	docker-compose -f docker-compose-pyspark.yml up -d;
	done

cassandra:
	echo "Starting Cassandra Service"; \
	docker-compose -f docker-compose-cassandra.yml up -d;
	done

kafka-down:
	echo "Stopping Kafka Service"; \
	docker-compose -f docker-compose-kafka.yml down --remove-orphans;
	done

pyspark-down:
	echo "Stopping Pyspark Service"; \
	docker-compose -f docker-compose-pyspark.yml down --remove-orphans;
	done

cassandra-down:
	echo "Stopping Cassandra Service"; \
	docker-compose -f docker-compose-cassandra.yml down --remove-orphans;
	done