package server;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServerResource {

    private final Map<String,Set<String>> networkData=new HashMap<>(); // Resource -> Set of Nodes
    private final Map<String,String> nodeAddresses=new HashMap<>(); // nodeId -> "IP:Port"
    private final ReentrantReadWriteLock lock=new ReentrantReadWriteLock();

    // Server attributes

    // DownloadLease ckass for downloading

    // registerNode()
    public void registerNode(String nodeId, String ip, int port) {
        lock.writeLock().lock();
        try {
            nodeAddresses.put(nodeId, ip + ":" + port);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // unregisterNode()

    // addResource()
    public void addResource(String nodeId, String resource) {
        lock.writeLock().lock();
        try {
            networkData.computeIfAbsent(resource, k -> new TreeSet<>()).add(nodeId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // getOnlineNodes()

    // getNetworkData()

    // getResourceLock()

    // requestToken()

    // requestTokensForNode

    // findOnlineOwner()

    // releaseToken()

    // getLogs
}
