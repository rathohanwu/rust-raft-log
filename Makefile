.PHONY: e2e-docker e2e-docker-failover e2e-docker-recovery docker-cluster-up docker-cluster-state docker-cluster-stop docker-cluster-start docker-cluster-down

e2e-docker:
	scripts/e2e-docker.sh

e2e-docker-failover:
	scripts/e2e-docker-failover.sh

e2e-docker-recovery:
	scripts/e2e-docker-recovery.sh

docker-cluster-up:
	scripts/docker-cluster-up.sh

docker-cluster-state:
	scripts/docker-cluster-state.sh

docker-cluster-stop:
	scripts/docker-cluster-stop.sh

docker-cluster-start:
	scripts/docker-cluster-start.sh

docker-cluster-down:
	scripts/docker-cluster-down.sh
