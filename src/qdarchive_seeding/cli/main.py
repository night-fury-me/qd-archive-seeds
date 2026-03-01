from __future__ import annotations

import typer

from qdarchive_seeding.cli.commands.seed import seed_app

app = typer.Typer(name="qdarchive", help="QDArchive seeding CLI.")
app.add_typer(seed_app, name="seed")

if __name__ == "__main__":
    app()
