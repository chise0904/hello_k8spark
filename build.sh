eval $(minikube docker-env)
docker build -t bank-spark-job:v1 .
