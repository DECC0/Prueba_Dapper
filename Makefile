.PHONY: reset-airflow init-airflow up-airflow down-airflow start

down-airflow:
	docker-compose down --volumes

reset-airflow: down-airflow
	sudo chown -R $$(id -u):$$(id -g) logs dags plugins || true
	rm -rf logs/* plugins/*
	find dags -mindepth 1 -type f -name '*.pyc' -delete
	mkdir -p logs plugins
	chmod 777 logs dags plugins

init-airflow:
	docker-compose run --rm webserver airflow db init
	docker-compose run --rm webserver \
	  airflow users create \
	    --username admin --password admin \
	    --firstname Admin --lastname User \
	    --role Admin --email admin@example.com

up-airflow:
	docker-compose up -d

start: reset-airflow init-airflow up-airflow
