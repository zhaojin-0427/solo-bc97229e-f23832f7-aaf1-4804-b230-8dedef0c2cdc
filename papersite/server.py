#!/usr/bin/env python3
"""Static paper site for local citation-crawl demos.

Serves plain static HTML files from ./site, plus three behaviours needed by
the demo corpus:

* /papers/slow.html    -- served only after a delay, so its (short) path to
                          paper X arrives *after* the long chain registered X
                          at a deeper depth ("late short path").
* /papers/broken.html  -- the FIRST request fails with HTTP 500; later
                          requests succeed ("one request failure", retryable).
* everything else      -- ordinary static files (text/html for .html).

State is in-memory only; restarting the container resets the flaky page.
"""

import http.server
import os
import socketserver
import threading
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "site")
PORT = int(os.environ.get("PORT", "8080"))

DELAY_SECONDS = {"/papers/slow.html": 2.5}
FLAKY_PATHS = {"/papers/broken.html"}

_lock = threading.Lock()
_already_failed: set[str] = set()


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def do_GET(self):
        path = self.path.split("?", 1)[0].split("#", 1)[0]
        if path in FLAKY_PATHS:
            with _lock:
                if path not in _already_failed:
                    _already_failed.add(path)
                    print(f"[papersite] {path} -> 500 (simulated transient failure)", flush=True)
                    self.send_error(500, "Simulated transient failure; a retry will succeed")
                    return
        delay = DELAY_SECONDS.get(path)
        if delay:
            print(f"[papersite] {path} -> delayed {delay}s", flush=True)
            time.sleep(delay)
        super().do_GET()

    def log_message(self, fmt, *args):
        print("[papersite] " + (fmt % args), flush=True)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    with Server(("", PORT), Handler) as httpd:
        print(f"[papersite] serving {ROOT} on :{PORT}", flush=True)
        httpd.serve_forever()
