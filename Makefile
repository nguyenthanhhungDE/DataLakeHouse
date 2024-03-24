to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_create:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/mysql_datasource.sql"

mysql_load:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/mysql_load.sql"

to_psql:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_psql_no_db:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres

psql_create:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /tmp/load_dataset/psql_datasource.sql -a