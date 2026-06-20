package common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// DownloadRequest is an entity that serves to keep a log of the download requests and transactions
public class DownloadRequest {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");
    private final String sourceNode;
    private final String destinationNode;
    private final String resourceName;
    private final LocalDateTime timestamp;
    private final boolean success;

    public DownloadRequest(String sourceNode, String destinationNode, String resourceName, boolean success) {
        this.sourceNode = sourceNode;
        this.destinationNode = destinationNode;
        this.resourceName = resourceName;
        this.success = success;
        this.timestamp = LocalDateTime.now();
    }

    public String getSourceNode() {
        return sourceNode;
    }

    public String getDestinationNode() {
        return destinationNode;
    }

    public String getResourceName() {
        return resourceName;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        String stato = success ? "ok" : "fallito";
        return String.format("- %s %s da: %s a: %s (%s)",
                timestamp.format(TIME_FORMATTER), resourceName, sourceNode, destinationNode, stato);
    }
}
