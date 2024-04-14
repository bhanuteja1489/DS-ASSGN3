start:
	make clean
	docker-compose up -d metadb
	docker-compose up -d shard_manager
	docker-compose up lb
build:
	docker-compose build 


clean:
	make clean_servers
	-docker-compose down

clean_servers:
	-docker rm -f $$(docker ps -aqf "ancestor=serverimg")

remove_images:
	-docker rmi -f serverimg lb

connect_db:
	mysql --host localhost -u kayden -P 30000 -pkayden@123
# to get logs of any container use:      docker logs -f CONTAINER_ID OR NAME
# mysql --host localhost -u kayden -P 30000 -pkayden@123 to check database
# use metadb;select * from MapT;