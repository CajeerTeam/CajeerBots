from pathlib import Path


def test_admin_static_files_exist_and_are_russian():
    root = Path("admin")
    assert (root / "index.html").exists()
    assert (root / "app.js").exists()
    assert (root / "style.css").exists()
    index = (root / "index.html").read_text(encoding="utf-8")
    assert 'lang="ru"' in index
    assert "Панель" in index
    assert "/readyz" in index
    assert "/dead-letters" in index


def test_admin_routes_are_declared():
    routes = Path("core/api_routes.py").read_text(encoding="utf-8")
    dispatcher = Path("core/api_dispatcher.py").read_text(encoding="utf-8")
    for path in ["/admin", "/admin/app.js", "/admin/style.css"]:
        assert path in routes
        assert path in dispatcher
