from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Input, Select, Static


class AuthForm(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("[bold]Authentication[/bold]")
        yield Static("Auth type:")
        yield Select(
            [
                ("None", "none"),
                ("API Key", "api_key"),
                ("Bearer Token", "bearer"),
                ("OAuth2", "oauth2"),
            ],
            prompt="Auth type",
            value="none",
            id="auth-type",
        )
        yield Static("Env var for API key/token:")
        yield Input(placeholder="e.g. ZENODO_TOKEN", id="auth-env-key")

    def get_data(self) -> dict[str, Any]:
        auth_type = str(self.query_one("#auth-type", Select).value or "none")
        env_key = self.query_one("#auth-env-key", Input).value
        env: dict[str, str] = {}
        if env_key:
            env["api_key"] = env_key
        return {"type": auth_type, "env": env}
