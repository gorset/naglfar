import naglfar
import BaseHTTPServer

class HelloWorldHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write('Hello, world')
        else:
            self.send_error(404)

if __name__ == '__main__':
    class ScheduledHTTPServer(naglfar.ScheduledMixIn, BaseHTTPServer.HTTPServer):
        pass
    BaseHTTPServer.test(HelloWorldHandler, ScheduledHTTPServer)
