#!/usr/bin/env bash
# Rolling restart of the API under load, on the cluster from up.sh. A client pod
# creates payments (a new idempotency key per request) for DURATION seconds
# while every API pod is replaced, then the script prints how many requests
# were sent and how many did not get a 201. Exits non-zero if any failed.
#   deploy/kind/up.sh && deploy/kind/rollout-load.sh
set -euo pipefail
RELEASE=${RELEASE:-payhook}
DURATION=${DURATION:-45}   # seconds of load; must outlast the rollout
WORKERS=${WORKERS:-4}      # parallel request loops in the client pod
POD=rollout-load
DEPLOY=deploy/$RELEASE-api

# Runs inside the client pod (busybox sh).
read -r -d '' CLIENT <<'SH' || true
end=$(( $(date +%s) + DURATION ))
for w in $(seq "$WORKERS"); do
  (
    ok=0; fail=0; i=0
    while [ "$(date +%s)" -lt "$end" ]; do
      i=$((i + 1))
      code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST "$URL/v1/payments" \
        -H "Idempotency-Key: rollout-$RUN-$w-$i" -H 'Content-Type: application/json' \
        -d '{"merchant_id":"rollout","amount":100,"currency":"KZT"}')
      if [ "$code" = 201 ]; then ok=$((ok + 1)); else fail=$((fail + 1)); echo "request failed: HTTP $code" >&2; fi
    done
    echo "$ok $fail" > "/tmp/worker-$w"
  ) &
done
wait
cat /tmp/worker-* | awk '{ ok += $1; fail += $2 } END { printf "total=%d failed=%d\n", ok + fail, fail }'
SH

kubectl delete pod "$POD" --ignore-not-found --wait >/dev/null
kubectl run "$POD" --image=curlimages/curl:8.16.0 --restart=Never \
  --env="URL=http://$RELEASE" --env="DURATION=$DURATION" --env="WORKERS=$WORKERS" --env="RUN=$(date +%s)" \
  --command -- sh -c "$CLIENT" >/dev/null
kubectl wait --for=condition=Ready "pod/$POD" --timeout=60s >/dev/null
sleep 5 # some load before the first pod goes away

echo "restarting $DEPLOY under load for ${DURATION}s"
kubectl rollout restart "$DEPLOY"
kubectl rollout status "$DEPLOY" --timeout=180s
if [ "$(kubectl get pod "$POD" -o jsonpath='{.status.phase}')" != Running ]; then
  echo "warning: the load ended before the rollout finished; raise DURATION" >&2
fi

kubectl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout="$((DURATION + 60))s" >/dev/null
result=$(kubectl logs "$POD" | tail -n 1)
kubectl logs "$POD" | grep 'request failed' | sort | uniq -c >&2 || true
kubectl delete pod "$POD" --wait=false >/dev/null
echo "$result"
[[ $result == *" failed=0" ]]
