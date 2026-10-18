# TCP Load Balancer in Go

## The Problem

Imagine a website that runs on one server. When lots of people use it at the same time, that server can become overloaded, slow down, or crash.

If it goes down, users may lose access to the website. Running multiple servers helps, but we still need a way to distribute users between them and avoid servers that aren't working.

## Our Solution

We're building a TCP Load Balancer in Go that acts like a traffic manager. It listens for incoming client connections, chooses a backend server using a method called round robin, and forwards the data between the client and that server.

It also checks whether backends are reachable and stops sending new connections to servers that are down.

With concurrent connections, health checks, and load testing, we're learning how to make this system handle multiple users more reliably instead of depending on one server.
