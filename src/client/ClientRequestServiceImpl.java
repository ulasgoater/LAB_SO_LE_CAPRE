package client;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import common.DownloadInfo;
import common.Rilevazione;
import services.ClientRequestService;
import services.DownloadResult;

// A class built to create seperation of concerns -> injects the resource class
public class ClientRequestServiceImpl implements ClientRequestService {
    private final ClientResource resource;
    private Socket aggregatorSocket;
    private PrintWriter out;
    private BufferedReader in;
    private final Object aggregatorLock = new Object();

    // Bug #1: Max retry attempts for robust download protocol
    private static final int MAX_DOWNLOAD_RETRIES = 50;

    public ClientRequestServiceImpl(ClientResource resource) {
        this.resource = resource;
    }

    @Override
    public List<Rilevazione> listLocalData(String nodeId) {
        return resource.getLocalData();
    }

    @Override
    public Rilevazione add(String nodeId, Rilevazione rilevazione) {
        resource.addRilevazione(rilevazione);
        synchronized (aggregatorLock) {
            if (out != null) {
                out.println("ADD " + rilevazione.getNome());
                // Bug #8: Read the ACK response to stay in sync with the protocol
                try {
                    in.readLine(); // ACK
                } catch (IOException e) {
                    System.out.println("Errore: ACK non ricevuto dall'aggregatore.");
                }
            }
        }
        return rilevazione;
    }

    @Override
    public List<String> listRemoteData() {
        synchronized (aggregatorLock) {
            try {
                out.println("LIST_REMOTE");
                List<String> res = new ArrayList<>();
                String line;
                while ((line = in.readLine()) != null && !line.equals("END")) {
                    res.add(line);
                }
                return res;
            } catch (IOException e) {
                System.out.println("Errore: connessione con l'aggregatore interrotta.");
                return new ArrayList<>();
            }
        }
    }

    @Override
    public List<String> listNodes() {
        synchronized (aggregatorLock) {
            try {
                out.println("LIST_NODES");
                List<String> res = new ArrayList<>();
                String line;

                while ((line = in.readLine()) != null && !line.equals("END")) {
                    res.add(line);
                }

                return res;

            } catch (IOException e) {
                System.out.println("Errore: connessione con l'aggregatore interrotta: " + e);
                return new ArrayList<>();
            }
        }
    }

    @Override
    public List<String> findResourceOwners(String resourceName) {
        synchronized (aggregatorLock) {
            try {
                out.println("FIND_RESOURCE " + resourceName);
                List<String> res = new ArrayList<>();
                String line;
                while ((line = in.readLine()) != null && !line.equals("END")) {
                    res.add(line);
                }
                return res;
            } catch (IOException e) {
                System.out.println("Errore: connessione con l'aggregatore interrotta.");
                return new ArrayList<>();
            }
        }
    }

    /**
     * Bug #1 (robust download retry loop) + Bug #7 (BUSY → wait and retry).
     *
     * Protocol:
     * 1. Ask aggregator for a token + target node address.
     * 2. If BUSY (target node occupied), wait and retry from step 1.
     * 3. Connect to the target node P2P and download.
     * 4. If P2P download fails (NOT_FOUND or connection error), release the token
     * with success=false (aggregator removes stale entry), then go back to step 1.
     * 5. If success, release the token with success=true.
     * 6. If aggregator says NOT_FOUND or NO_OWNER, stop — resource unavailable.
     */
    @Override
    public DownloadResult download(String resourceName, String localNodeId) {
        int retries = 0;
        while (retries++ < MAX_DOWNLOAD_RETRIES) {
            String peerIp = null;
            int peerPort = -1;

            // Step 1: chiedi all'aggregatore un nodo sorgente
            synchronized (aggregatorLock) {
                try {
                    out.println("DOWNLOAD " + resourceName);
                    String response = in.readLine();
                    if (response == null)
                        return DownloadResult.CONNECTION_ERROR;

                    switch (response) {
                        case "NOT_FOUND":
                            return DownloadResult.NOT_FOUND;
                        case "NO_OWNER":
                            return DownloadResult.NO_OWNER;
                    }

                    if (!response.startsWith("SUCCESS"))
                        return DownloadResult.CONNECTION_ERROR;

                    String[] parts = response.split("\\s+", 3);
                    if (parts.length < 3)
                        return DownloadResult.CONNECTION_ERROR;

                    String peerAddress = parts[2];
                    int separator = peerAddress.lastIndexOf(':');
                    if (separator <= 0 || separator == peerAddress.length() - 1)
                        return DownloadResult.CONNECTION_ERROR;

                    peerIp = peerAddress.substring(0, separator);
                    peerPort = Integer.parseInt(peerAddress.substring(separator + 1));

                } catch (IOException | NumberFormatException e) {
                    System.out.println("Errore di comunicazione con l'aggregatore: " + e.getMessage());
                    return DownloadResult.CONNECTION_ERROR;
                }
            }

            // Step 2: tenta il download P2P
            String content = performP2PDownload(peerIp, peerPort, resourceName);
            boolean success = content != null;

            if (success) {
                resource.addRilevazione(new Rilevazione(resourceName, content));
                // Notifica l'aggregatore della nuova rilevazione locale
                synchronized (aggregatorLock) {
                    if (out != null) {
                        out.println("ADD " + resourceName);
                        try {
                            in.readLine(); // consume ACK to keep protocol in sync
                        } catch (IOException e) {
                            // non-critical
                        }
                    }
                }
            }

            // Step 3: rilascia il token (con esito)
            synchronized (aggregatorLock) {
                if (out != null)
                    out.println("RELEASE_TOKEN " + resourceName + " " + success);
            }

            if (success)
                return DownloadResult.SUCCESS;

            // Step 4: fallito → il ciclo riparte, l'aggregatore ha già rimosso il nodo
            // fallito
            System.out.println("Nodo non raggiungibile, riprovo con un altro nodo...");
        }
        // Esauriti i tentativi massimi di retry
        return DownloadResult.NOT_FOUND;
    }

    @Override
    public DownloadResult downloadFromNode(String targetNode, String localNodeId) {

        List<DownloadInfo> downloads = new ArrayList<>();

        // 1. LEGGI TUTTE LE RIGHE SUCCESS (DENTRO IL LOCK)
        synchronized (aggregatorLock) {
            try {
                out.println("DOWNLOAD " + targetNode);

                String line;
                while (true) {
                    line = in.readLine();

                    if (line == null) {
                        System.out.println("CLIENT: Connessione chiusa dal server durante il download multiplo");
                        break;
                    }

                    if (line.equals("END")) {
                        break;
                    }

                    String[] parts = line.split("\\s+");

                    if (parts.length < 5) {
                        System.out.println("RIGA SUCCESS MALFORMATA: " + line);
                        continue;
                    }

                    String owner = parts[1];
                    String ipPort = parts[2];
                    String resource = parts[4];

                    downloads.add(new DownloadInfo(owner, ipPort, resource));
                }

            } catch (IOException e) {
                System.out.println("Errore di comunicazione con l'aggregatore: " + e.getMessage());
                return DownloadResult.CONNECTION_ERROR;
            }
        }

        // 2. FAI I P2P (FUORI DAL LOCK)
        for (DownloadInfo d : downloads) {
            String ip = d.ipPort.substring(0, d.ipPort.lastIndexOf(':'));
            int port = Integer.parseInt(d.ipPort.substring(d.ipPort.lastIndexOf(':') + 1));

            String content = performP2PDownload(ip, port, d.resource);
            boolean success = (content != null && !content.equals("NOT_FOUND"));

            if (success) {
                resource.addRilevazione(new Rilevazione(d.resource, content));

                // 3. MANDI ADD (DENTRO IL LOCK)
                synchronized (aggregatorLock) {
                    if (out != null) {
                        out.println("ADD " + d.resource);
                        try {
                            in.readLine(); // ACK — must be consumed to keep protocol in sync
                        } catch (IOException e) {
                            // non-critical
                        }
                    }
                }
            }

            // RELEASE TOKEN ALWAYS
            synchronized (aggregatorLock) {
                if (out != null) {
                    out.println("RELEASE_TOKEN " + d.resource + " " + success);
                }
            }
        }

        return DownloadResult.SUCCESS;
    }

    private String performP2PDownload(String ip, int port, String resourceName) {
        try (Socket peerSocket = new Socket(ip, port);
                PrintWriter peerOut = new PrintWriter(peerSocket.getOutputStream(), true);
                BufferedReader peerIn = new BufferedReader(new InputStreamReader(peerSocket.getInputStream()))) {
            peerOut.println(resourceName);
            return peerIn.readLine(); // may be null (EOF) or "NOT_FOUND"
        } catch (IOException e) {
            System.out.println("Errore di connessione P2P: " + e.getMessage());
            return null;
        }
    }

    /**
     * Bug #2: After registration, send all existing local resources to the
     * aggregator.
     */
    @Override
    public String connectToAggregator(String host, int port, String nodeId, int peerPort) throws Exception {
        this.aggregatorSocket = new Socket(host, port);
        this.out = new PrintWriter(aggregatorSocket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(aggregatorSocket.getInputStream()));

        // Registrazione con porta P2P
        this.out.println("REGISTER " + peerPort);

        // Legge l'ID assegnato dal server
        String assignedId = in.readLine();
        System.out.println("ID assegnato dal server: " + assignedId);

        // Invia subito le rilevazioni pre-esistenti e consuma gli ACK
        List<Rilevazione> preloaded = resource.getLocalData();
        for (Rilevazione r : preloaded) {
            this.out.println("ADD " + r.getNome());
            in.readLine(); // consume ACK to keep protocol in sync
        }

        return assignedId;
    }

    @Override
    public void disconnect(String nodeId) {
        synchronized (aggregatorLock) {
            if (out != null) {
                out.println("DISCONNECT");
            }
        }
        try {
            if (aggregatorSocket != null)
                aggregatorSocket.close();
        } catch (IOException e) {
        }
    }
}
