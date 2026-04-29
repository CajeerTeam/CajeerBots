from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.request import Request, urlopen

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

    def fetch_latest(self) -> ReleaseManifest | None:
        req = Request(self._api_url(), headers={"Accept": "application/vnd.github+json", "User-Agent": "CajeerBots-Updater"})
        with urlopen(req, timeout=self.timeout_seconds) as response:  # noqa: S310 - URL controlled by configured repo.
            data = json.loads(response.read().decode("utf-8"))
        if isinstance(data, list):
            release = next((item for item in data if self.allow_prerelease or not item.get("prerelease")), None)
            if release is None:
                return None
        else:
            release = data
            if release.get("prerelease") and not self.allow_prerelease:
                return None
        assets = release.get("assets") or []
        manifest_asset = next((item for item in assets if str(item.get("name", "")).endswith(".release.json")), None)
        if manifest_asset:
            manifest_req = Request(manifest_asset["browser_download_url"], headers={"User-Agent": "CajeerBots-Updater"})
            with urlopen(manifest_req, timeout=self.timeout_seconds) as response:  # noqa: S310
                return ReleaseManifest.from_dict(json.loads(response.read().decode("utf-8")))
        tar_asset = next((item for item in assets if str(item.get("name", "")).endswith(".tar.gz")), None)
        return ReleaseManifest(
            name="CajeerBots",
            version=str(release.get("tag_name") or release.get("name") or "").lstrip("v"),
            channel=self.channel,
            python=">=3.11",
            db_contract="cajeer.bots.db.v1",
            event_contract="cajeer.bots.event.v1",
            requires_migration=False,
            artifacts=[ReleaseArtifact(name=str(tar_asset.get("name")), url=str(tar_asset.get("browser_download_url")), size=tar_asset.get("size"))] if tar_asset else [],
        )
