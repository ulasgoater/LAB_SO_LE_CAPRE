package server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import common.DownloadRequest;
import services.ServerRequestService;

public class Server {
    private final int port;
    private final ServerRequestService service;
    private volatile boolean running = true;
    private ServerSocket serverSocket;
    private final Set<Socket> activeClientSockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    });
    private static final Set<Integer> usedIds = ConcurrentHashMap.newKeySet();
    private static final PriorityQueue<Integer> freeIds = new PriorityQueue<>();
    static {
        freeIds.add(0);
        freeIds.add(1);
        freeIds.add(2);
        freeIds.add(3);
    }

    public Server(int port) {
        this.port = port;
        this.service = new ServerRequestServiceImpl(new ServerResource());
    }

    public void start() {
        // listener thread must NOT be daemon so JVM stays alive
        Thread listenerThread = new Thread(this::listenForClients);
        listenerThread.setDaemon(false);
        listenerThread.start();
        handleConsole();
    }

    private void listenForClients() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Aggregator in ascolto sulla porta " + port);
            while (running) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setKeepAlive(true);
                activeClientSockets.add(clientSocket);
                executor.submit(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            if (running) {
                System.out.println("Errore o socket chiuso: " + e.getMessage());
            }
        } finally {
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    private static synchronized int assignNodeId() {
        // Se ci sono ID liberi (0–3 o riciclati), usa quelli
        if (!freeIds.isEmpty()) {
            int id = freeIds.poll();
            usedIds.add(id);
            return id;
        }

        // Altrimenti crea un nuovo ID crescente
        int id = usedIds.size();
        while (usedIds.contains(id)) {
            id++;
        }
        usedIds.add(id);
        return id;
    }

    private static synchronized void releaseNodeId(int id) {
        usedIds.remove(id);

        // Se è uno dei primi 4, torna disponibile
        if (id < 4) {
            freeIds.add(id);
        }
    }

    private void handleConsole() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (running) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String input = scanner.nextLine().trim();
                switch (input) {
                    case "listdata":
                        System.out.println("Risorse:");
                        service.listData().forEach(System.out::println);
                        break;

                    case "log":
                        System.out.println("Risorse scaricate:");
                        for (DownloadRequest req : service.logData()) {
                            System.out.println(req.toString());
                        }
                        break;
                    case "quit":
                        running = false;
                        System.out.println("Chiusura aggregatore...");
                        try {
                            if (serverSocket != null) {
                                serverSocket.close();
                            }
                        } catch (IOException e) {
                            // Ignore
                        }
                        for (Socket s : activeClientSockets) {
                            try {
                                s.close();
                            } catch (IOException e) {
                                // Ignore
                            }
                        }
                        activeClientSockets.clear();
                        executor.shutdownNow();
                        return;
                    default:
                        if (!input.isEmpty()) {
                            System.out.println("Comando non riconosciuto.");
                        }
                        break;
                }
            }
        }
    }

    private class ClientHandler implements Runnable {
        private final Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String connectedNodeId = null;
            try (Socket s = socket;
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
                String line;
                while (running && !socket.isClosed()) {
                    line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    String[] parts = line.split("\\s+");
                    switch (parts[0]) {
                        case "COUNT_NODES":
                            out.println(service.listNodes().size());
                            out.println("END");
                            break;

                        case "REGISTER":
                            if (parts.length >= 2) {
                                int p2pPort = Integer.parseInt(parts[1]);
                                String ip = socket.getInetAddress().getHostAddress();
                                int id = assignNodeId();
                                connectedNodeId = "peer" + id;
                                out.println(connectedNodeId); // <-- fondamentale
                                service.registerNode(connectedNodeId, ip, p2pPort);
                                System.out.println(
                                        "Registrato nuovo nodo: " + connectedNodeId + " (" + ip + ":" + p2pPort + ")");
                            }
                            break;

                        // Bug #2: Client sends its pre-existing resources after REGISTER
                        case "REGISTER_RESOURCES":
                            if (connectedNodeId != null && parts.length >= 2) {
                                for (int i = 1; i < parts.length; i++) {
                                    service.addResource(connectedNodeId, parts[i]);
                                }
                                out.println("ACK");
                            }
                            break;

                        case "LIST_REMOTE":
                            service.listData().forEach(out::println);
                            out.println("END");
                            break;

                        case "LIST_NODES":
                            service.listNodes().forEach(out::println);
                            out.println("END");
                            break;

                        case "ADD":
                            if (connectedNodeId != null && parts.length == 2) {
                                String resource = parts[1];
                                System.out.println("ADD ricevuto da " + connectedNodeId + " per risorsa " + resource);
                                service.addResource(connectedNodeId, resource);
                                out.println("ACK");
                            } /*
                               * else if (connectedNodeId != null && parts.length >= 2) {
                               * service.addResource(connectedNodeId, parts[1]);
                               * out.println("ACK");
                               * }
                               */
                            break;

                        case "DOWNLOAD":
                            if (connectedNodeId != null && parts.length >= 2) {

                                String target = parts[1].trim();

                                // DOWNLOAD MULTIPLO
                                if (target.matches("peer\\d+")) {

                                    var results = service.requestTokensForNode(target, connectedNodeId);

                                    for (String res : results) {
                                        // res è nel formato: owner ip:port resource
                                        String[] p = res.split("\\s+");
                                        String owner = p[0];
                                        String ipPort = p[1];
                                        String resource = p[2];

                                        String successLine = "SUCCESS " + owner + " " + ipPort + " RESOURCE "
                                                + resource;

                                        System.out.println(successLine);
                                        out.println(successLine);
                                        out.flush();
                                    }
                                    out.println("END");
                                    out.flush();
                                }

                                // DOWNLOAD SINGOLO
                                else {
                                    String res = service.requestToken(target, connectedNodeId);

                                    if (res.equals("BUSY") || res.equals("NOT_FOUND") || res.equals("NO_OWNER")) {
                                        out.println(res);
                                    } else {
                                        out.println(res);
                                    }
                                }
                            }
                            break;

                        case "RELEASE_TOKEN":
                            if (connectedNodeId != null && parts.length >= 3) {
                                String tokenResource = parts[1];
                                boolean tokenSuccess = Boolean.parseBoolean(parts[2]);
                                service.releaseToken(tokenResource, connectedNodeId, tokenSuccess);
                                System.out.println("Token rilasciato per " + tokenResource + " da " + connectedNodeId
                                        + " (successo: " + tokenSuccess + ")");
                            }
                            break;

                        case "DISCONNECT":
                            System.out.println("Nodo " + connectedNodeId + " disconnesso esplicitamente.");
                            return; // exits run(), triggers finally block for cleanup
                    }
                }
            } catch (IOException | NumberFormatException e) {
                // Sockets occasionally close abruptly; logging facilitates debugging protocol
                // errors
                System.err.println("Info di connessione client ("
                        + (connectedNodeId != null ? connectedNodeId : "unknown") + "): " + e.getMessage());
            } finally {
                activeClientSockets.remove(socket);
                if (connectedNodeId != null) {
                    int id = Integer.parseInt(connectedNodeId.replace("peer", ""));
                    releaseNodeId(id);
                    service.unregisterNode(connectedNodeId);
                }
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Uso: java Master <porta>");
            return;
        }
        try {
            int port = Integer.parseInt(args[0]);
            new Server(port).start();
        } catch (NumberFormatException e) {
            System.out.println("La porta deve essere un numero.");
        }
    }
}
