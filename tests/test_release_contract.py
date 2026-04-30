from pathlib import Path


def test_release_contract_has_required_structure():
    root = Path.cwd()
    assert (root / "core").is_dir()
    assert (root / "bots/telegram/main.py").is_file()
    assert (root / "bots/discord/main.py").is_file()
    assert (root / "bots/vkontakte/main.py").is_file()
    assert (root / "wiki/Home.md").is_file()
    assert not (root / "migrations").exists()


def test_entrypoints_are_executable():
    root = Path.cwd()
    for relative in ["run.sh", "install.sh", "setup_wizard.py"]:
        assert not (root / relative).exists(), f"root wrapper не должен существовать при scripts-only layout: {relative}"
    for path in (root / "scripts").glob("*.sh"):
        assert path.stat().st_mode & 0o111, str(path)
