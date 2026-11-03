"""Static checks of the dashboard Compose service and RabbitMQ stats config (text-based)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
CONF = (ROOT / "deploy" / "rabbitmq" / "90-pizza.conf").read_text(encoding="utf-8")


def _dashboard_block() -> str:
    start = COMPOSE.index("\n  dashboard:")
    end = COMPOSE.index("\nvolumes:", start)
    return COMPOSE[start:end]


def test_dashboard_port_localhost_only() -> None:
    assert '"127.0.0.1:8000:8000"' in _dashboard_block()
    assert "0.0.0.0:8000" not in COMPOSE


def test_dashboard_command_and_anchor() -> None:
    block = _dashboard_block()
    assert '["python", "-m", "pizza.dashboard"]' in block
    assert "<<: *station" in block
    assert "stop_grace_period: 10s" in block
    assert "start_period: 60s" in block
    assert "/healthz" in block
    assert "RABBITMQ_MGMT_URL: http://rabbitmq:15672" in block


def test_rabbitmq_conf_mounted_readonly() -> None:
    assert "./deploy/rabbitmq/90-pizza.conf:/etc/rabbitmq/conf.d/90-pizza.conf:ro" in COMPOSE


def test_rabbitmq_conf_settings() -> None:
    lines = [ln.strip() for ln in CONF.splitlines() if ln.strip() and not ln.startswith("#")]
    assert lines == ["collect_statistics_interval = 1000", "consumer_timeout = 7200000"]
