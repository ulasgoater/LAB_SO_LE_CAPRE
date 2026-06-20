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

    private class ClientHandler implements Runnable{
        private final Socket socket;

        public ClientHandler(Socket socket){
            this.socket=socket;
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
    // listenForClients()
    // handleConsole()
    // ClientHandler class that implements runnable
    // run()
    // main class