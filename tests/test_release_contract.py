from pathlib import Path


def test_release_contract_has_required_structure():
    root = Path.cwd()
    assert (root / "core").is_dir()
    assert (root / "bots/telegram/bot").is_dir()
    assert (root / "bots/discord/bot").is_dir()
    assert (root / "bots/vkontakte/bot").is_dir()
    assert (root / "wiki/Home.md").is_file()
    assert not (root / "migrations").exists()


def test_entrypoints_are_executable():
    root = Path.cwd()
    for relative in ["run.sh", "install.sh", "setup_wizard.py"]:
        assert (root / relative).stat().st_mode & 0o111, relative
    for path in (root / "scripts").glob("*.sh"):
        assert path.stat().st_mode & 0o111, str(path)
