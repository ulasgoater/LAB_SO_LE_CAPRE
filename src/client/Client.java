package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import common.Rilevazione;
import services.DownloadResult;

// Class of client - lascio in commento le funzioni che dovrete completare per la clean archittura
public class Client {
    private final String host;
    private final int port;
    private String nodeId;
    private final String dataDir;
    private final ClientResource resource;
    private final ClientRequestServiceImpl service;
    private volatile boolean running;
    private final Set<Socket> activePeerSockets = ConcurrentHashMap.newKeySet();
    // crea una thread pool --> riutilizza quelli già esistenti se sono liberi
    private final ExecutorService executor = Executors.newCachedThreadPool(
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable);
                    thread.setDaemon(true);
                    return thread;
                }
            });

    /*
     * Meccanismo di mutua esclusione per le richieste P2P in ingresso (F1/F2/F3).
     *
     * Deadlock avoidance (F4): il pattern di locking è asimmetrico — solo il lato
     * SERVENTE (handlePeerConnection) acquisisce il semaforo, mentre il lato
     * RICHIEDENTE (performP2PDownload in ClientRequestServiceImpl) apre un Socket
     * senza acquisire alcun lock locale. Questo spezza la condizione di attesa
     * circolare (circular-wait) dei criteri di Coffman, rendendo il deadlock
     * strutturalmente impossibile anche quando due nodi A e B si richiedono
     * reciprocamente in contemporanea.
     */

    // Only one thread can access the critical section at a time
    private final Semaphore p2Semaphore = new Semaphore(1);

    public Client(String host, int port) {
        this(host, port, null);
    }

    public Client(String host, int port, String dataDir) {
        this.host = host;
        this.port = port;
        this.nodeId = null;
        this.dataDir = dataDir;
        this.resource = new ClientResource(); // archivio rilevazioni ogni nodo
        this.service = new ClientRequestServiceImpl(this.resource); // per le comunicazioni
        this.running = true;
        if (dataDir != null) {
            this.resource.loadFromDirectory(dataDir);
        }
    }

    public void start() {
        try (ServerSocket p2pSocket = new ServerSocket(0)) { // automatically allocated port
            int p2pPort = p2pSocket.getLocalPort(); // per prendere la porta che è stata assegnata
            Runnable p2pTask = new Runnable() {
                @Override
                public void run() {
                    listenP2P(p2pSocket);
                }
            };
            Thread p2pThread = new Thread(p2pTask);
            p2pThread.setDaemon(false); // così fin quando è attivo non si chiude la Jvm
            p2pThread.start();
            this.nodeId = service.connectToAggregator(host, this.port, nodeId, p2pPort);
            if (dataDir == null) {
                this.resource.useDirectory("data-" + nodeId);
            }
            System.out.println("Connesso all'aggregatore al " + host + ": " + port + " come " + nodeId + " (P2P su "
                    + p2pPort + " )");
            handleConsole();
        } catch (Exception e) {
            System.out.println("Errore nella connessione all'agregatore");
        } finally {
            shutdown();
        }
    }

    // metodo p2p per far si che gli altri nodi si possano collegare per scaricare
    // le rilevazioni
    private void listenP2P(ServerSocket p2pSocket) {
        while (running) {
            try {
                Socket peer = p2pSocket.accept();
                activePeerSockets.add(peer); // aggiunge al Set i nodi attivi

                Runnable peerTask = new Runnable() {
                    @Override
                    public void run() {
                        handlePeerConnection(peer);
                    }
                };
                executor.submit(peerTask);
            } catch (IOException e) {
                if (running) {
                    System.out.println();
                }
            }
        }
    }

    /*
     * handleConsole serve per gestire le istruzioni scritte dall'utente nel
     * terminale
     */

    private void handleConsole() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (running) {
                System.out.print(">");
                if (!scanner.hasNextLine()) {
                    break;
                }

                String input = scanner.nextLine().trim();
                if (input.isEmpty()) {
                    continue;
                }

                String[] parts = input.split("\\s+", 3);
                String cmd = parts[0];

                switch (cmd) {
                    case "help":
                        System.out.println("Comandi disponibili:");
                        System.out.println("  listdata [local|remote]         : Mostra le risorse locali o remote");
                        System.out.println("  listnodes                       : Mostra i nodi attivi sulla rete");
                        System.out.println(
                                "  find <nome risorsa>             : Mostra i nodi che possiedono una determinata risorsa");
                        System.out.println(
                                "  add <nome risorsa> <contenuto>  : Aggiunge una nuova rilevazione al nodo corrente");
                        System.out.println(
                                "  download <nome risorsa | peerX> : Scarica una singola risorsa o tutte le risorse di un nodo");
                        System.out.println(
                                "  quit                            : Disconnette il nodo e chiude l'applicazione");
                        System.out.println("  help                            : Mostra questo messaggio di aiuto");
                        break;

                    case "listdata":
                        if (parts.length > 1 && parts[1].equals("local")) {
                            System.out.println("Risorse: ");
                            for (Rilevazione r : service.listLocalData(nodeId)) {
                                System.out.println("- " + r.getNome());
                            }
                        } else if (parts.length > 1 && parts[1].equals("remote")) {
                            System.out.println("Risorse: ");
                            service.listRemoteData().forEach(System.out::println);
                        } else {
                            System.out.println("Uso listdata [local|remote]");
                        }
                        break;

                    case "listnodes":
                        System.out.println("Nodi attivi: ");
                        service.listNodes().forEach(System.out::println);
                        break;

                    case "find":
                        if (parts.length == 2) {
                            System.out.println("Nodi che possiedono " + parts[1] + ":");
                            service.findResourceOwners(parts[1]).forEach(node -> System.out.println("- " + node));
                        } else {
                            System.out.println("Uso: find <nome risorsa>");
                        }
                        break;

                    case "add":
                        if (parts.length == 3) {
                            String nome = parts[1];
                            String contenuto = parts[2];
                            Rilevazione r = new Rilevazione(nome, contenuto);
                            service.add(nodeId, r);
                            System.out.println("Risorsa " + nome + " aggiunta.");

                        } else {
                            System.out.println("Uso: add <nome risorsa> <contenuto>");
                        }
                        break;

                    case "download":
                        if (parts.length == 2) {
                            String target = parts[1].trim();
                            DownloadResult result = target.matches("peer\\d+") // matches() controlla che target abbia
                                                                               // il pattern corretto
                                    ? service.downloadFromNode(target, nodeId)
                                    : service.download(target, nodeId); // uno per la singola rilevazione e l'altro per
                                                                        // le rilevazioni di un dato nodo

                            switch (result) {
                                case SUCCESS:
                                    System.out.println("Download completato e token rilasciato");
                                    break;
                                case NOT_FOUND:
                                    System.out.println("Risorsa non trovata sulla rete (download fallito)");
                                    break;
                                case NO_OWNER:
                                    System.out.println("Nessun nodo online possiede questa risorsa (download fallito)");
                                    break;
                                case CONNECTION_ERROR:
                                default:
                                    System.out.println("Download fallito, errore di connessione");
                                    break;
                            }

                        } else {
                            System.out.println("Uso: download <nome risorsa | peerX>");
                        }
                        break;

                    case "quit":
                        shutdown();
                        break;
                    default:
                        System.out.println("Comando non riconosciuto");
                        break;
                }
            }
        }
    }

    // metodo per la chiusura del nodo sensore con notifica all' aggr.
    private void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        service.disconnect(nodeId);
        for (Socket s : activePeerSockets) {
            try {
                s.close();
            } catch (IOException e) {
                System.out.println("Errore chiusura socket P2P: " + e.getMessage());
            }
        }
        activePeerSockets.clear();
        executor.shutdownNow();
    }

    // gestisce la connessione con un alrto nodo
    private void handlePeerConnection(Socket peer) {
        try {
            p2Semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            try {
                peer.close();
            } catch (IOException ex) {
                System.out.println("Errore chiusura socket: " + ex.getMessage());
            }
            return;
        }

        try (Socket p = peer;
                BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                PrintWriter out = new PrintWriter(p.getOutputStream(), true)) {

            String resourceName = in.readLine();
            if (resourceName != null) {
                resource.getContent(resourceName).ifPresent(out::println);
            }

        } catch (IOException e) {
            System.out.println("Errore comunicazione P2P: " + e.getMessage());
        } finally {
            activePeerSockets.remove(peer);
            p2Semaphore.release(); // Rilascia sempre, anche in caso di eccezione
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.out.println("Uso: java Client <indirizzo aggregator> <porta aggregator> [cartella dati]");
            return;
        }
        try {
            String dataDir = args.length == 3 ? args[2] : null;
            new Client(args[0], Integer.parseInt(args[1]), dataDir).start();
        } catch (NumberFormatException e) {
            System.out.println("La porta deve essere un numero.");
        }
    }
}
