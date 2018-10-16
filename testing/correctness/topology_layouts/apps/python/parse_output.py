import struct
from components import Message

f = open("received.txt", 'rb')

while True:
    d = f.read(4)
    if not d:
        break
    h = struct.unpack('>I', d)[0]
    print(Message.decode(f.read(h)))

exit(1)
