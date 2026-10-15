.PHONY: test unit up down logs demo lint

API_PORT ?= 8080
API := http://localhost:$(API_PORT)
export API_PORT

test:            ## all tests, including e2e with Docker
	go test -race -count=1 ./...

unit:            ## unit tests only
	go test -race ./internal/...

lint:
	go vet ./...
	golangci-lint run

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f api relay worker

demo:            ## create a payment and mark it succeeded
	@ID=$$(curl -s -X POST $(API)/v1/payments -H 'Idempotency-Key: demo-'$$RANDOM \
		-d '{"merchant_id":"shop-1","amount":1500000,"currency":"KZT"}' | sed -E 's/.*"id":"([^"]+)".*/\1/'); \
	echo "payment $$ID"; \
	go run ./cmd/sendwebhook -url $(API)/v1/webhooks/halyk -payment $$ID; echo; \
	curl -s $(API)/v1/payments/$$ID; echo
