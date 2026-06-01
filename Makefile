.PHONY: run test docker-build docker-run clean

INPUT    ?= $(CURDIR)/data/input
OUTPUT   ?= $(CURDIR)/data/output
CONTRACT ?= $(CURDIR)/Contract_rules.yaml

run:
	bash run.sh $(INPUT) $(OUTPUT) $(CONTRACT)

test:
	python -m pytest tests/ -v

docker-build:
	docker compose build

docker-run:
	docker compose up

clean:
	rm -rf data/output
