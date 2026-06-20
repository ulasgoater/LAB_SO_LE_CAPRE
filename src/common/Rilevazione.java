package common;

import java.time.LocalDateTime;
import java.util.UUID;

public class Rilevazione {
    private final UUID id;
    // this way the user can choose what rilevazione he wants to add
    private final String nome;
    private final String contenuto;
    private final LocalDateTime timestamp;

    public Rilevazione(String nome, String contenuto) {
        this.id = UUID.randomUUID();
        this.nome = nome;
        this.contenuto = contenuto;
        this.timestamp = LocalDateTime.now();
    }

    public String getNome() {
        return nome;
    }

    public String getContenuto() {
        return contenuto;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String toString() {
        return "Rilevazione{" +
                "id=" + id +
                ", nome='" + nome + '\'' +
                ", contenuto='" + contenuto + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
