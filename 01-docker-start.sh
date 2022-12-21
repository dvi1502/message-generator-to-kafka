sudo rm ./data -R
$HOME/.docker/cli-plugins/docker-compose --file docker-compose-kafka-cluster.yml up  --build --detach --wait

