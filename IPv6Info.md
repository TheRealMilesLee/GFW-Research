# The basics of IPv6
A brand new protocol from scratch - totally different from the IPV4

## Current problem with IPV4

IPv4 uses 32-bit addresses, which allows for approximately 4.3 billion unique addresses.
The solution to this is simple, just using NAT on each level could significantly solve
this problem. However, as the demand for internet-connected devices continues to grow,
NAT alone cannot address the fundamental issue of limited IPv4 address space.

The main problem with NAT lies in its performance issues and scalability, as well as
the fact that NAT networks can leave certain gaming services with limited and disconnected
connections. Because of its configurability and controllability, carrier-grade NAT has been
deployed on a large scale in China.

However, as mentioned earlier, the problem of address exhaustion remains unresolved.
Using NAT only temporarily expands the IP address pool like caching, but it is not a permanent
solution. With the massive popularity of smart homes in recent years, the IPv4 address problem
has become a priority again. This time the IPv6 protocol is needed to save the day.

## What is IPv6
IPv6 is a brand new protocol that if totally different from IPv4 and does not compatiable with IPv4
It provides an identification and location system for devices connected to a network.
Its main advantages over IPv4, the previous version, include:

1. **Expanded Address Space**: IPv6 uses 128-bit addresses, allowing for a vastly larger number of unique addresses
compared to the 32-bit addresses used in IPv4.

2. **Improved Security**: IPv6 includes built-in support for IPsec (Internet Protocol Security), which provides end-to-end
security, authentication, and encryption of data packets.

3. **Auto-configuration**: IPv6 simplifies network configuration by enabling devices to automatically configure their own
IP addresses and other network parameters without the need for manual intervention or DHCP servers.

4. **Efficient Routing and Packet Processing**: IPv6 reduces the size of routing tables and simplifies packet processing,
leading to more efficient routing and improved network performance.

5. **Quality of Service (QoS) Support**: IPv6 includes features for prioritizing different types of traffic, allowing for
better QoS management and ensuring that critical applications receive the necessary bandwidth and performance.

6. **Future-Proofing**: With the exhaustion of IPv4 addresses, IPv6 provides a scalable solution to accommodate the ever-
expanding number of devices connecting to the internet, ensuring the continued growth and evolution of the network.

In summary, IPv6 offers a larger address space, improved security features, simplified network configuration, enhanced
routing efficiency, support for QoS, and future scalability, making it essential for the continued growth and development
of the internet.

## IPv6 cheat sheet ##

### Address length and format ###

The IPv6 address is **128 bits** (i.e. 16 bytes) long and is written in **8 groups of 2 bytes** in hexadecimal numbers
separated by colons:

    FDDD:F00D:CAFE:0000:0000:0000:0000:0001

Leading zeros of each block can be omitted, the above address can hence be written like this:

    FDDD:F00D:CAFE:0:0:0:0:1

We can abbreviate whole blocks of zeros with `::` and write:

    FDDD:F00D:CAFE::1

This can only be done *once* in order to void ambiguity:

    FF:0:0:0:1:0:0:1 (correct)
    FF::1:0:0:1 (correct)
    FF:0:0:0:1::1 (correct)
    FF::1::1 (ambiguous, wrong)

### Protocols ###

| Number | Protocol  | Purpose                                   |
| ------ | --------- | ----------------------------------------- |
|  58    | IPv6-ICMP | Information, Error reporting, diagnostics |
|  6     | TCP       | Stateful - controls if packets arrived    |
| 17     | UDP       | Stateless - streaming applications etc.   |

### Ways to  assign IPv6 addresses ###

**Static** - fixed address
**SLAAC** - Stateless Address Autoconfiguration (host generates itself)
**DHCPv6** - Dynamic host configuration protocol (assigned by central server)

### Scopes and special addresses ###

**GLOBAL** - everything (i.e. the whole internet)
**UNIQUE LOCAL** - everything in our LAN (behind the internet gateway)
**LINK LOCAL** - (will never be routed, valid in one collision domain, i.e. on the same switch)

| range     | Purpose                                        |
| --------- | ---------------------------------------------- |
| ::1/128   | Loopback address (localhost)                   |
| ::/128    | unspecified address                            |
| 2000::/3  | GLOBAL unicast (Internet)                      |
| FC00::/7  | Unique-local (LAN)                             |
| FE80::/10 | Link-Local Unicast (same switch)               |

Always use the smallest possible scope for communication
A host can have **multiple** addresses in different scopes

### Subnetting ###

| bits (MSB)      | Purpose                                        |
| --------------- | ----------------------- |
| First 48 bits:  | **Network** address     |
| Next 16 bits:   | **Subnet** address      |
| Last 64 bits:   | **Device** address      |

**Network+Subnet = Prefix**

The following address

    2003:1000:1000:1600:1234::1

would have the network `2003:1000:1000`, the subnet `1600`, so together the prefix `2003:1000:1000:1600`. If the ISP provider **delegated** a part of the prefix to me (e.g. `2003:1000:1000:1600/56`) then I could use the subnets from `2003:1000:1000:1600` to `2003:1000:1000:16FF` for my own purposes (i.e. define 256 subnets in this example)

### IPv6 addresses in URIs/URLs ###

Because IPv6 address notation uses colons to separate hextets, it is necessary to encase the address in square brackets in URIs. For example `http://[2a00:1450:4001:82a::2004]`. If you want to specify a port, you can do so as normal using a colon: `http://[2a00:1450:4001:82a::2004]:80`.
