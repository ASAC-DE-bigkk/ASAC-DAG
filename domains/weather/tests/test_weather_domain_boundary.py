import ast
from pathlib import Path


DOMAINS_DIR = Path(__file__).resolve().parents[2]


def _production_modules(domain: str) -> tuple[Path, ...]:
    domain_root = DOMAINS_DIR / domain
    return tuple(
        path
        for path in sorted(domain_root.rglob("*.py"))
        if "tests" not in path.relative_to(domain_root).parts
        and "__pycache__" not in path.parts
    )


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    return imported


def test_traffic_transform_modules_do_not_import_weather_modules():
    violations = {
        str(path.relative_to(DOMAINS_DIR)): sorted(
            name
            for name in _imports(path)
            if name == "weather"
            or name.startswith("weather.")
            or name.startswith("weather_")
        )
        for path in _production_modules("traffic")
    }

    assert not {path: names for path, names in violations.items() if names}


def test_weather_transform_modules_do_not_import_traffic_modules():
    violations = {
        str(path.relative_to(DOMAINS_DIR)): sorted(
            name
            for name in _imports(path)
            if name == "traffic"
            or name.startswith("traffic.")
            or name.startswith("traffic_")
        )
        for path in _production_modules("weather")
    }

    assert not {path: names for path, names in violations.items() if names}
