SHELL := /bin/bash
BIN := bin/litellm-lite
COMPOSE := docker compose -f deploy/compose/docker-compose.yml
export $(shell [ -f .env ] && sed 's/=.*//' .env)
-include .env

.PHONY: build up down infra migrate seed serve test load lint clean

build:
	go build -o $(BIN) ./cmd/litellm-lite

infra:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

migrate: build
	$(BIN) migrate

seed: build
	$(BIN) seed

serve: build
	$(BIN) serve

# One-shot local bring-up: containers, schema, seed data.
up: infra migrate seed
	@echo
	@echo "Ready. Run 'make serve' then try: curl -s localhost:8080/v1/gateway/key -H \"Authorization: Bearer \$$(cat data/seed-key)\""

test:
	go test ./...

# Load test against the running gateway using the seeded mock key.
load: build
	$(BIN) load --key "$$(cat data/seed-key)" --rps $${RPS:-500} --duration $${DURATION:-20s} --concurrency $${CONC:-512}

clean: down
	rm -rf bin data
