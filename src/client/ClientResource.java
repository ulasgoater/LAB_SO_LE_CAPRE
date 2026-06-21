package client;

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


    /*DA AGGIUNGERE */

    // loadFromDirectory


    // getContent
}
