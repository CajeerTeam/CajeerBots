.PHONY: install doctor migrate run-all ci release
install:
	./scripts/install.sh
doctor:
	./scripts/doctor.sh --offline
migrate:
	./scripts/migrate.sh
run-all:
	./scripts/run.sh all
ci:
	python -m compileall -q cajeer_bots bots modules plugins
	EVENT_SIGNING_SECRET=ci-secret python -m cajeer_bots doctor --offline
release:
	./scripts/release.sh
