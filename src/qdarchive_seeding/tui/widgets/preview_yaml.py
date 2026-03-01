from __future__ import annotations

from typing import Any

import yaml
from textual.widgets import Static


class PreviewYaml(Static):
    def update_config(self, config_dict: dict[str, Any]) -> None:
        try:
            text = yaml.dump(config_dict, default_flow_style=False, sort_keys=False)
        except Exception:
            text = str(config_dict)
        self.update(f"```yaml\n{text}```")
