package server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import services.ServerRequestService;

public class Server {
    private final int port;
    private final ServerRequestService service;
    private volatile boolean running=true;
    private ServerSocket serverSocket;

    public Server(int port) {
        this.port=port;
        this.service=new ServerRequestServiceImpl(new ServerResource());//fare classe
    }

    public void start() {
        Thread listenerThread=new Thread(this::listenForClients);
        listenerThread.setDaemon(false);
        listenerThread.start();
    }

    private void listenForClients() {
    try{
        serverSocket=new ServerSocket(port);
        System.out.println("Server in ascolto sulla porta " + port);

        while(running) {
            Socket clientSocket=serverSocket.accept();
            System.out.println("Nuovo client connesso");
            new Thread(new ClientHandler(clientSocket)).start();
        }

    }catch(IOException e) {
        e.printStackTrace();
    }
    }

    private static synchronized int assignNodeId(){
    // Se ci sono ID liberi (0–3 o riciclati), usa quelli
    if (!freeIds.isEmpty()) {
        int id=freeIds.poll();
        usedIds.add(id);
        return id;
    }

    // Altrimenti crea un nuovo ID crescente
    int id=usedIds.size();
    while(usedIds.contains(id)) {
        id++;
    }
    usedIds.add(id);
    return id;
    }

    private static synchronized void releaseNodeId(int id) {
    usedIds.remove(id);

    // Se è uno dei primi 4, torna disponibile
    if (id<4) {
        freeIds.add(id);
    }
    }


    private class ClientHandler implements Runnable{
        private final Socket socket;

        public ClientHandler(Socket socket){
            this.socket=socket;
        }
        @Override
        public void run(){
            String connectedNodeId=null;
            try (Socket s=socket;
                 BufferedReader in=new BufferedReader(new InputStreamReader(s.getInputStream()));
                 PrintWriter out =new PrintWriter(s.getOutputStream(),true)) {
                String line;
                while(running && !socket.isClosed()){
                    line=in.readLine();
                    if(line==null) {
                        break;
                        }
                    String[] parts=line.split("\\s+");
                    switch (parts[0]){
                        case "COUNT_NODES":
                            out.println(service.listNodes().size());
                            out.println("END");
                            break;

                        case "REGISTER":
                        if (parts.length>=2) {
                            int p2pPort=Integer.parseInt(parts[1]);
                            String ip=socket.getInetAddress().getHostAddress();
                            int id=assignNodeId();
                            connectedNodeId= "Utente"+id;
                            out.println(connectedNodeId);
                            service.registerNode(connectedNodeId,ip,p2pPort);
                            System.out.println("Registrato nuovo nodo: "+connectedNodeId+" ("+ip+":"+p2pPort+")");
                        }
                        break;

                        // Bug #2: Client sends its pre-existing resources after REGISTER
                        case "REGISTER_RESOURCES":
                            if(connectedNodeId!= null&&parts.length>=2) {
                                for (int i=1; i<parts.length; i++) {
                                    service.addResource(connectedNodeId, parts[i]);
                                }
                                out.println("ACK");
                            }
                            break;

                        //mancano: operazioni per letture liste, add, download
                        
                        case "DISCONNECT":
                            System.out.println("Nodo " + connectedNodeId + " disconnesso esplicitamente.");
                            return; // exits run(), triggers finally block for cleanup
                    }
                }
            } catch (IOException | NumberFormatException e) {
                // Sockets occasionally close abruptly; logging facilitates debugging protocol errors
                System.err.println("Info di connessione client ("+(connectedNodeId != null ? connectedNodeId : "unknown")+"): "+e.getMessage());
            } finally {
                activeClientSockets.remove(socket);
                if (connectedNodeId!=null) {
                    int id=Integer.parseInt(connectedNodeId.replace("Utente", ""));
                    releaseNodeId(id); fare metodo per ascoltare il client
                    service.unregisterNode(connectedNodeId);
                }
            }
        }
    } //fare run

    public static void main(String[] args){
        if(args.length!=1){
            System.out.println("Uso: java Master <porta>");
            return;
        }
        try{
            int port=Integer.parseInt(args[0]);
            new Server(port).start();
        }catch(NumberFormatException e){
            System.out.println("La porta deve essere un numero.");
        }
    }
}
    // server initialize
    // start()
    // listenForClients() -> da finire
    // handleConsole() -> da finire
    // ClientHandler class that implements runnable
    // run()
    // main class


