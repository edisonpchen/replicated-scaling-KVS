:: my batch file
docker stop kvs1
docker rm kvs1
docker stop kvs2
docker rm kvs2
docker stop kvs3
docker rm kvs3
start cmd /k docker run --publish 8080:8080 --net=kv_subnet --ip=10.10.0.2 --name="kvs1" --env ADDRESS="10.10.0.2:8080" kvs:1.0
start cmd /k docker run --publish 8081:8080 --net=kv_subnet --ip=10.10.0.3 --name="kvs2" --env ADDRESS="10.10.0.3:8080" kvs:1.0
start cmd /k docker run --publish 8082:8080 --net=kv_subnet --ip=10.10.0.4 --name="kvs3" --env ADDRESS="10.10.0.4:8080" kvs:1.0
timeout /t 10 /nobreak
python test_assignment3.py 8080:10.10.0.2:8080 8081:10.10.0.3:8080 8082:10.10.0.4:8080
PAUSE
