package server;

import java.net.ServerSocket;

import services.ServerRequestService;

public class Server {
    private final int port;
    private final ServerRequestService service;
    private volatile boolean running = true;
    private ServerSocket serverSocket;
}
