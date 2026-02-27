"""Git repository operations using gitpython."""

import logging
import shutil
from pathlib import Path
from typing import Any

import yaml
from git import Repo
from git.exc import GitCommandError

from app.infrastructure.exceptions import GitCloneError
from app.schemas.platform_config import PlatformConfig

logger = logging.getLogger(__name__)

PLATFORM_CONFIG_FILENAME = ".platform.yaml"


class GitClient:
    """Handles git clone, checkout, .platform.yaml parsing, and cleanup."""

    def __init__(self, clone_base_dir: str) -> None:
        self.clone_base_dir = Path(clone_base_dir)
        self.clone_base_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _build_clone_url(repo_url: str, token: str | None) -> str:
        """Build the clone URL, injecting a GitHub PAT if provided."""
        if not token:
            return repo_url
        # https://github.com/user/repo -> https://x-access-token:TOKEN@github.com/user/repo
        if repo_url.startswith("https://"):
            return repo_url.replace(
                "https://",
                f"https://x-access-token:{token}@",
                1,
            )
        return repo_url

    def clone_repo(
        self,
        repo_url: str,
        branch: str,
        target_dir_name: str,
        github_token: str | None = None,
    ) -> Path:
        """Clone a repository and checkout the specified branch.

        Returns the path to the cloned repository directory.
        """
        repo_path = self.clone_base_dir / target_dir_name
        if repo_path.exists():
            shutil.rmtree(repo_path)

        clone_url = self._build_clone_url(repo_url, github_token)

        logger.info(
            "Cloning %s (branch: %s) into %s",
            repo_url,
            branch,
            repo_path,
        )
        try:
            Repo.clone_from(
                clone_url,
                str(repo_path),
                branch=branch,
                depth=1,
            )
        except GitCommandError as exc:
            raise GitCloneError(
                repo_url=repo_url,
                branch=branch,
                detail=str(exc),
            ) from exc

        logger.info("Repository cloned successfully to %s", repo_path)
        return repo_path

    def parse_platform_config(self, repo_path: Path) -> PlatformConfig:
        """Find and parse the .platform.yaml file in the repository root.

        Returns a validated PlatformConfig instance.
        Raises FileNotFoundError if the file is missing.
        Raises ValueError if the YAML is invalid or fails validation.
        """
        config_file = repo_path / PLATFORM_CONFIG_FILENAME
        if not config_file.exists():
            msg = (
                f"{PLATFORM_CONFIG_FILENAME} not found in repository root "
                f"at {repo_path}"
            )
            raise FileNotFoundError(msg)

        logger.info("Parsing %s", config_file)
        with config_file.open("r", encoding="utf-8") as f:
            raw_config: Any = yaml.safe_load(f)

        if not isinstance(raw_config, dict):
            msg = f"{PLATFORM_CONFIG_FILENAME} must be a YAML mapping"
            raise TypeError(msg)

        return PlatformConfig(**raw_config)

    @staticmethod
    def cleanup(repo_path: Path) -> None:
        """Remove the cloned repository directory."""
        if repo_path.exists():
            shutil.rmtree(repo_path)
            logger.info("Cleaned up cloned repository at %s", repo_path)
