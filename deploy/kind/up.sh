#!/usr/bin/env bash
# Local Kubernetes run: kind cluster + NATS (official chart) + Postgres + payhook chart.
set -euo pipefail
cd "$(dirname "$0")/../.."
CLUSTER=${CLUSTER:-payhook}

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
kubectl config use-context "kind-$CLUSTER"

docker build -t payhook:dev .
kind load docker-image payhook:dev --name "$CLUSTER"

helm repo add nats https://nats-io.github.io/k8s/helm/charts/ >/dev/null 2>&1 || true
helm repo update nats >/dev/null
helm upgrade --install nats nats/nats -f deploy/kind/nats-values.yaml --wait --timeout 5m

kubectl apply -f deploy/kind/postgres.yaml
kubectl rollout status statefulset/postgres --timeout=180s

# metrics-server so the HPA has CPU numbers (kubelet TLS is self-signed in kind)
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.8.0/components.yaml >/dev/null
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null 2>&1 || true

helm upgrade --install payhook deploy/helm/payhook -f deploy/kind/values-kind.yaml --wait --timeout 5m
helm test payhook --logs
