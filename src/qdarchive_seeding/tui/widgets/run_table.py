from __future__ import annotations

from textual.widgets import DataTable


class RunTable(DataTable):
    def on_mount(self) -> None:
        self.add_columns("Asset URL", "Status", "Bytes", "Error")

    def update_asset(
        self,
        asset_url: str,
        status: str,
        bytes_downloaded: int = 0,
        error: str = "",
    ) -> None:
        short_url = asset_url[-60:] if len(asset_url) > 60 else asset_url
        for row_key, row_data in self.rows.items():
            cells = list(self.get_row(row_key))
            if cells and cells[0] == short_url:
                self.update_cell(row_key, "Status", status)
                self.update_cell(row_key, "Bytes", str(bytes_downloaded))
                if error:
                    self.update_cell(row_key, "Error", error)
                return
        self.add_row(short_url, status, str(bytes_downloaded), error)
