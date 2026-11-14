import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from weather_observations.sources import HttpJsonSource


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"records": [{"source": "http", "observed_at": "2026-11-01T00:00:00Z", "payload": {"id": "1"}}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def test_http_source_reads_records_and_lands_raw_response(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source = HttpJsonSource(f"http://127.0.0.1:{server.server_port}/records", raw_dir=str(tmp_path / "raw"))
        records = source.read()
        assert len(records) == 1
        assert records[0]["payload"]["id"] == "1"
        assert list((tmp_path / "raw").glob("*.json"))
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_http_records_flow_through_event_time_coordination(tmp_path):
    """HTTP ingestion keeps working and now records watermarks/compensation."""
    from weather_observations.application import run_url

    class _WeatherHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"records": [{
                "source": "http",
                "observed_at": "2026-11-01T12:00:00Z",
                "payload": {
                    "station_id": "H-1",
                    "metric": "temperature",
                    "source_observed_at": "2026-11-01T11:00:00Z",
                    "value": 21.5,
                    "unit": "C",
                },
            }]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _WeatherHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = run_url(
            f"http://127.0.0.1:{server.server_port}/records",
            str(tmp_path / "db.sqlite3"),
            raw_dir=str(tmp_path / "raw"),
            allowed_lateness="1h",
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    assert result["processed"] == 1
    assert result["rejected"] == 0
    assert result["observations"] == 1
    assert result["compensations"] == 1
    assert list((tmp_path / "raw").glob("*.json"))
