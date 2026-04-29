.PHONY: install doctor doctor-online run run-all api worker bridge telegram discord vk vkontakte adapters modules plugins commands db-status lint test ci release clean

PYTHON ?= python

install: ## Установить зависимости проекта
	./scripts/install.sh

doctor: ## Проверить проект без внешних сервисов
	./scripts/doctor.sh --offline

doctor-online: ## Проверить проект с PostgreSQL и токенами адаптеров
	./scripts/doctor.sh

run: run-all ## Запустить все включённые адаптеры

run-all: ## Запустить все включённые адаптеры
	./scripts/run.sh all

api: ## Запустить HTTP API платформы
	./scripts/run.sh api

worker: ## Запустить рабочий процесс
	./scripts/run.sh worker

bridge: ## Запустить шину событий
	./scripts/run.sh bridge

telegram: ## Запустить только Telegram-адаптер
	./scripts/run.sh telegram

discord: ## Запустить только Discord-адаптер
	./scripts/run.sh discord

vk: vkontakte ## Алиас для запуска ВКонтакте-адаптера

vkontakte: ## Запустить только ВКонтакте-адаптер
	./scripts/run.sh vkontakte

adapters: ## Показать адаптеры
	$(PYTHON) -m core adapters

modules: ## Показать модули
	$(PYTHON) -m core modules

plugins: ## Показать плагины
	$(PYTHON) -m core plugins

commands: ## Показать команды
	$(PYTHON) -m core commands

db-status: ## Показать статус внешнего управления БД
	$(PYTHON) -m core db-status

lint: ## Выполнить быструю статическую проверку Python-синтаксиса
	$(PYTHON) -m compileall -q core bots modules plugins tests
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

test: ## Запустить тесты каркаса
	$(PYTHON) -m pytest -q

ci: lint doctor adapters modules plugins commands ## Запустить локальный CI-набор

release: ## Собрать релизный архив
	./scripts/release.sh

clean: ## Удалить временные каталоги сборки
	rm -rf dist build *.egg-info .pytest_cache
