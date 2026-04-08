COMPOSE=docker compose

.PHONY: up down logs ps restart

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=150

ps:
	$(COMPOSE) ps

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d --build
