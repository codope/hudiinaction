#!/usr/bin/env python3
"""Tiny HTTP receiver for Chapter 9's post-commit callback example.

`python3 -m http.server` does not log POST bodies, so this script exists solely to print
the exact JSON payload Hudi's HoodieWriteCommitHttpCallback sends after a commit. Not for
production use: no auth check beyond echoing the API key header, no persistence.
"""
import http.server
import json
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9192


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        print(f"POST {self.path}")
        print(f"HUDI-CALLBACK-KEY: {self.headers.get('HUDI-CALLBACK-KEY')}")
        print("Body:")
        print(json.dumps(json.loads(body), indent=2))
        sys.stdout.flush()
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # keep stdout clean for the payload prints above


http.server.HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
