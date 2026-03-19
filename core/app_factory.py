from __future__ import annotations

import importlib
import pkgutil

from flask import Flask

import moduli


def _load_blueprints(app: Flask) -> None:
    for module_info in pkgutil.iter_modules(moduli.__path__):
        module_name = module_info.name
        qualified_name = f"{moduli.__name__}.{module_name}"

        try:
            module = importlib.import_module(qualified_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[ERRORE] modulo non importato: {qualified_name} -> {exc}")
            continue

        create_blueprint = getattr(module, "create_blueprint", None)
        if create_blueprint is None:
            print(f"[SKIP] create_blueprint() non trovato: {qualified_name}")
            continue

        try:
            blueprint = create_blueprint()
            app.register_blueprint(blueprint)
            print(f"[OK] blueprint registrato: {qualified_name} -> {blueprint.name}")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERRORE] blueprint non registrato: {qualified_name} -> {exc}")


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "local-test-host"
    _load_blueprints(app)
    return app
