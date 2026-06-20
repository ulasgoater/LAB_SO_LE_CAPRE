package common;

import java.io.Serializable;

// DownlaodInfo is an entity that keeps info about the downlaoding
public class DownloadInfo implements Serializable {
    public static final long serialVersionUID = 1L;
    public final String owner;
    public final String ipPort;
    public final String resource;

    public DownloadInfo(String owner, String ipPort, String resource) {
        this.owner = owner;
        this.ipPort = ipPort;
        this.resource = resource;
    }

    public String getOwner() {
        return owner;
    }

    public String getIpPort() {
        return ipPort;
    }

    public String getResource() {
        return resource;
    }

    @Override
    public String toString() {
        return "DownloadInfo{" +
                "owner='" + owner + '\'' +
                ", ipPort='" + ipPort + '\'' +
                ", resource='" + resource + '\'' +
                '}';
    }
}
