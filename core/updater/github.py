from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen, urlretrieve

from core.updater.manifest import ReleaseArtifact, ReleaseManifest


@dataclass(frozen=True)
class GitHubReleaseSource:
    repo: str = "CajeerTeam/CajeerBots"
    channel: str = "stable"
    allow_prerelease: bool = False
    timeout_seconds: int = 10

    def _api_url(self) -> str:
        if self.channel == "nightly":
            return f"https://api.github.com/repos/{self.repo}/releases"
        return f"https://api.github.com/repos/{self.repo}/releases/latest"

    def _load_release(self) -> dict[str, object] | None:
        req = Request(self._api_url(), headers={"Accept": "application/vnd.github+json", "User-Agent": "CajeerBots-Updater"})
        with urlopen(req, timeout=self.timeout_seconds) as response:  # noqa: S310 - repo is configured by operator.
            data = json.loads(response.read().decode("utf-8"))
        if isinstance(data, list):
            return next((item for item in data if self.allow_prerelease or not item.get("prerelease")), None)
        if data.get("prerelease") and not self.allow_prerelease:
            return None
        return data

    def fetch_latest(self) -> ReleaseManifest | None:
        release = self._load_release()
        if release is None:
            return None
        assets = release.get("assets") or []
        manifest_asset = next((item for item in assets if str(item.get("name", "")).endswith(".release.json")), None)
        if manifest_asset:
            manifest_req = Request(str(manifest_asset["browser_download_url"]), headers={"User-Agent": "CajeerBots-Updater"})
            with urlopen(manifest_req, timeout=self.timeout_seconds) as response:  # noqa: S310
                manifest = ReleaseManifest.from_dict(json.loads(response.read().decode("utf-8")))
        else:
            tar_asset = next((item for item in assets if str(item.get("name", "")).endswith(".tar.gz")), None)
            manifest = ReleaseManifest(
                name="CajeerBots",
                version=str(release.get("tag_name") or release.get("name") or "").lstrip("v"),
                channel=self.channel,
                python=">=3.11",
                db_contract="cajeer.bots.db.v1",
                event_contract="cajeer.bots.event.v1",
                requires_migration=False,
                artifacts=[],
            )
            if tar_asset:
                manifest.artifacts.append(ReleaseArtifact(name=str(tar_asset.get("name")), url=str(tar_asset.get("browser_download_url")), size=tar_asset.get("size")))
        # Заполняем URL/size из assets, если release.json содержит только name/sha256.
        by_name = {str(item.get("name")): item for item in assets}
        patched = []
        for artifact in manifest.artifacts:
            asset = by_name.get(artifact.name, {})
            patched.append(ReleaseArtifact(artifact.name, artifact.url or str(asset.get("browser_download_url") or ""), artifact.sha256, artifact.size or asset.get("size")))
            sig_asset = by_name.get(artifact.name + ".sig")
            if sig_asset and not any(item.name == artifact.name + ".sig" for item in patched):
                patched.append(ReleaseArtifact(artifact.name + ".sig", str(sig_asset.get("browser_download_url") or ""), "", sig_asset.get("size")))
        return ReleaseManifest(manifest.name, manifest.version, manifest.channel, manifest.python, manifest.db_contract, manifest.event_contract, manifest.requires_migration, patched)

    def download_artifact(self, artifact: ReleaseArtifact, target_dir: Path) -> Path:
        if not artifact.url:
            raise ValueError(f"у artifact нет URL: {artifact.name}")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / artifact.name
        urlretrieve(artifact.url, target)  # noqa: S310 - URL comes from GitHub Releases asset.
        return target
