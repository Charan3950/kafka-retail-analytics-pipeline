.PHONY: up down reset logs test ps bad-event

up:
	docker compose up --build -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f producer processor sink

test:
	docker compose run --rm --no-deps producer pytest -q

ps:
	docker compose ps

bad-event:
	docker compose run --rm producer python -m src.send_bad_event

