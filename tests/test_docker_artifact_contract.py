from pathlib import Path


def test_dockerfile_copies_runtime_assets():
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    assert "COPY admin ./admin" in dockerfile
    assert "compatibility.yaml" in dockerfile
    assert "COPY configs ./configs" in dockerfile


def test_admin_assets_exist_for_docker_image():
    assert Path("admin/index.html").is_file()
    assert Path("admin/app.js").is_file()
    assert Path("admin/style.css").is_file()
    assert Path("compatibility.yaml").is_file()
