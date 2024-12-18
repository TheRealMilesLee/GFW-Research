import socket
import platform
from scapy.all import sniff, TCP, IP
from threading import Thread
import ssl

def ignore_rst_packet(packet):
  if packet.haslayer(TCP) and packet[TCP].flags == "R":
    print(f"Ignored RST packet from {packet[IP].src} to {packet[IP].dst}:{packet[TCP].dport}")

def get_default_interface():
  system = platform.system()
  if system == "Windows":
    return "Ethernet"
  elif system == "Darwin":  # macOS
    return "en0"
  else:
    return "eth0"

def start_sniffing(interface=None):
  if interface is None:
    interface = get_default_interface()
  print(f"Sniffing on interface: {interface}")
  sniff(iface=interface, filter="tcp", prn=ignore_rst_packet, store=False)

def make_request():
  try:
    for port in [80, 443]:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.settimeout(5)
      host = "google.com"
      s.connect((host, port))
      print(f"Connected to {host}:{port}")

      if port == 80:
        request = "GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n"
        s.send(request.encode())
      else:
        context = ssl.create_default_context()
        s = context.wrap_socket(s, server_hostname=host)
        request = "GET / HTTP/1.1\r\nHost: google.com\r\nConnection: close\r\n\r\n"
        s.send(request.encode())

      while True:
        data = s.recv(4096)
        if not data:
          break
        print(f"Received from port {port}: {data.decode()}")
      s.close()
  except Exception as e:
    print(f"Connection error: {e}")

if __name__ == "__main__":
  sniff_thread = Thread(target=start_sniffing, daemon=True)
  sniff_thread.start()

  make_request()
