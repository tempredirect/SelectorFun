__author__ = 'gareth'
import socket
import time

class MyClient:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.sock.connect(("localhost", 2001))

    def send(self, message):
        totalsent = 0
        msglen = len(message)
        while totalsent < msglen:
            sent = self.sock.send(message[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

cli = MyClient()

cli.connect()

cli.send("Wibble Wobble\n")
cli.send("Hello world\n")
cli.send("""This is a multi
line request""")

time.sleep(1)
cli.send("Hello world\n")
