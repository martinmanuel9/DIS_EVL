# Define the default target (first target in the file)
.DEFAULT_GOAL := help

# List all Docker Compose YAML files in the current directory
COMPOSE_FILES := $(wildcard *.yml)

# Define targets and their actions
help:
	@echo "Available targets:"
	@echo "  env-up      		- Start all services"
	@echo "  env-down     		- Stop and remove all containers"
	@echo "  logs     		- View container logs"
	@echo "  status   		- Show container status"
	@echo "  port-reset   		- Reset ports"
	@echo "  help     		- Show this help message"

env-up:
	@for file in $(COMPOSE_FILES); do \
		echo "Starting services in $$file"; \
		docker-compose -f $$file up -d; \
	done
	@make status
	sleep 20
	./env.sh

env-down:
	@for file in $(COMPOSE_FILES); do \
		echo "Stopping and removing containers in $$file"; \
		docker-compose -f $$file down --remove-orphans; \
		docker image prune --all; \
	done
	@make status


port-reset:
	lsof -i -u $(shell whoami)
	lsof -i -u $(shell whoami) | awk 'NR>1 {print $$2}' | xargs kill -9
	

logs:
	@for file in $(COMPOSE_FILES); do \
		echo "Viewing logs for containers in $$file"; \
		docker-compose -f $$file logs -f; \
	done

status:
	@echo "-----------------------------------------------------------------------------------------------------------------------------------------"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}"; 
	@echo "-----------------------------------------------------------------------------------------------------------------------------------------"\
	done

	


	
