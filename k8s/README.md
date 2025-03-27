gcloud container clusters get-credentials gke --region=<region> --project=<project>

gcloud compute ssh gke--bastion --tunnel-through-iap --project=<project> --zone=<zone> --billing-project=<project> --ssh-flag="-4 -L8888:localhost:8888 -N -q -f"

kubectl get ns


kubectl apply -f k8s/k8s_storage_class.yaml
kubectl apply -f k8s/k8s_stateful_set.yaml

The topic needs to be created manually in kafka.

If kafka is running in a container on a compute instance:
SSH into the Compute Instance.

`docker ps` to get the container id.

`docker exec -it <container_id> /bin/bash` to get into the container.

`/bin/kafka-topics --bootstrap-server 127.0.0.1:9093 --topic taskworker --create --partitions 4 --replication-factor 1` to create the topic.
