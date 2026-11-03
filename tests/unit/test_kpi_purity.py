import ast
from pathlib import Path

import pytest

import pizza.kpi as kpi_pkg

FORBIDDEN = {
    "confluent_kafka",
    "aio_pika",
    "asyncio",
    "time",
    "os",
    "socket",
    "random",
    "structlog",
}
MODULES = ["fold", "metrics", "theory", "stats", "bottleneck", "frozen", "due", "compare"]


@pytest.mark.parametrize("name", MODULES)
def test_modules_have_no_io_imports(name: str) -> None:
    path = Path(kpi_pkg.__file__).parent / f"{name}.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots = {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            roots = {(node.module or "").split(".")[0]}
        else:
            roots = set()
        assert not (roots & FORBIDDEN), f"{name} imports {roots & FORBIDDEN}"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in {"now", "utcnow"}, f"{name} calls {node.func.attr}"
