#!/usr/bin/env python3
import time
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


class FooHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        print("POST to {0}".format(self.path))
        clen = int(self.headers["content-length"])
        print(clen)
        print(self.rfile.read(clen))
        print("-" * 80)

        self.send_response(200, "OK")
        self.end_headers()

    def do_GET(self):
        print("GET of {0}".format(self.path))
        self.send_response(200, "OK")
        self.end_headers()


if __name__ == "__main__":
    print("sleeping...")
    time.sleep(float(sys.argv[1]) if len(sys.argv) > 1 else 0.0)
    print("serving!")
    HTTPServer(("", 8080), FooHandler).serve_forever()
