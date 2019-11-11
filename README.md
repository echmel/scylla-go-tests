FOr running scylla nodes:
```
docker run --name main-scylla -d scylladb/scylla
docker run --name scylla-node2 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' main-scylla)"
docker run --name scylla-node3 -d scylladb/scylla --seeds="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' main-scylla)"
```

For checking cluster:
```
docker exec -it main-scylla nodetool describecluster
docker exec -it main-scylla nodetool status
```
After create database with test table and run the app.