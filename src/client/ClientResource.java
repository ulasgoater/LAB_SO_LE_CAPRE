package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import common.Rilevazione;

public class ClientResource {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();  //serve per la gestione delle risorse condivise
    private final Map<String, Rilevazione> localData = new HashMap<>(); // Archivio per le rilevazioni



    /*serve per aggiungerer una rilevazione al nodo: prende il lock, 
    scrive sulla mappa la rilevazione con chiave il suo nome e poi rilascia il lock
    */
    public void addRilevazione(Rilevazione r){
        lock.writeLock().lock();
        try {
            localData.put(r.getNome(), r);
        } finally{
            lock.writeLock().unlock();
        }
    }
    /*
        questo ci da la lista con tutte le rilevazioni dentro l'archivio di un nodo.
        readLock e non writeLock così possono leggere contemporaneamente.
    */
    public List<Rilevazione> getLocalData(){
        lock.readLock().lock();
        try {
            return new ArrayList<>(localData.values());
        }finally{
            lock.readLock().unlock();
        } 
    }

    /*
        metodo per caricare le rilevazioni "pre-allocate" da cartella nell'archivio del nodo.
        Il nome del file  = nome rilevazione e contenuto file = valore rilevazione 

     */

    public void loadFromDirectory(String directoryPath){
        File dir = new File(directoryPath);
        if(!dir.exists() || !dir.isDirectory()){
            return;
        }
        File[] files = dir.listFiles(); //prendiamo tutte le rilevazioni nella cartella
        if(files == null){
            return;
        }

        lock.writeLock().lock();
        try{
            for(File f : files){ // per ogni file, leggiamo riga per riga e concateniamo per creare un unico contenuto e poi aggiungiamo all' archivio come nuova rilevazione 
                if(f.isFile()){
                    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                        StringBuilder content = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null ) {
                            if(content.length() > 0){
                                content.append("\n");
                            }
                            content.append(line);
                        }

                        String nome = f.getName();
                        localData.put(nome, new Rilevazione(nome, content.toString()));
                    } catch (IOException e) {
                        System.out.println("Errore nella lettura del file: " + f.getName());
                    }
                }
            }
        } finally{
            lock.writeLock().unlock();
        }
    }




    /*DA AGGIUNGERE */



    // getContent
}

