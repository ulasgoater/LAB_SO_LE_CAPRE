package client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Class of client - lascio in commento le funzioni che dovrete completare per la clean archittura
public class Client {
    private final String host;
    private final int port;
    private String nodeId;
    private final ClientResource resource;
    private final ClientRequestServiceImpl service;
    private volatile boolean running;
    private final Set<Socket> activePeerSockets= ConcurrentHashMap.newKeySet();;


    public Client(String host, int port){
        this.host = host;
        this.port = port;
        this.nodeId = null;
        this.resource = new ClientResource(); //archivio rilevazioni ogni nodo
        this.service = new ClientRequestServiceImpl(this.resource); //per le comunicazioni
        this.running = true;
    }

    public void start(){
        try(ServerSocket p2pSocket = new ServerSocket(0)){

            int p2pPort = p2pSocket.getLocalPort(); //per prendere la porta che è stata assegnata
            Runnable p2pTask = new Runnable() {
                @Override
                public void run(){
                    listenP2P(p2pSocket);
                }
            };
            Thread p2pThread = new Thread(p2pTask);
            p2pThread.setDaemon(false);  // così fin quando è attivo non si chiude la Jvm
            p2pThread.start();
            this.nodeId = service.connectToAggregator(host, p2pPort, nodeId, p2pPort);
            System.out.println("Connesso all'aggregatore al " + host + ": " + port + " come " + nodeId + " (P2P su " + p2pPort + " )" );

        }catch(Exception e){
            System.out.println("Errore nella connessione all'agregatore");
        } finally{
            // da aggiungere metodo per la chiusura
        }
    }


    //metodo p2p per far si che gli altri nodi si possano collegare per scaricare le rilevazioni
    private void listenP2P(ServerSocket p2pSocket){
        while(running){
            try {
                Socket peer = p2pSocket.accept();
                activePeerSockets.add(peer); //aggiunge al Set i nodi attivi 

                Runnable peerTask = new Runnable() {
                    @Override
                    public void run(){
                    // Devo aggiungere metodo per la gestione della singola connessione 
                    }

                };
            } catch (IOException e) {
                if(running){
                    System.out.println();
                }
            }
        }
    }


    /**
     * DEVO AGGIUNGERE METODO PER LA GESTIONE DELLA CONNESSIONE,
     *  PER LA CHIUSURA, PER I COMANDI DA CONSOLE.
     * VARI ED EVENTUALI.
     * 
     */


}


    // initialize client

    // handlePeerConnection()

    // handleConsole

    // shutdown

    // main