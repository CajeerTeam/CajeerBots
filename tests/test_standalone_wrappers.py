from pathlib import Path


def test_standalone_wrappers_exist():
    root = Path.cwd()
    for path in [
        "bots/telegram/main.py",
        "bots/discord/main.py",
        "bots/vkontakte/main.py",
        "bots/telegram/bot/main.py",
        "bots/discord/bot/main.py",
        "bots/vkontakte/bot/main.py",
    ]:
        text = (root / path).read_text(encoding="utf-8")
        assert "CORE_COMMANDS" in text
        assert "core.cli" in text
