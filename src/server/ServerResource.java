package server;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import common.DownloadRequest;

public class ServerResource {
    private final Map<String, Set<String>> networkData = new HashMap<>(); // Resource -> Set of Nodes
    private final Map<String, String> nodeAddresses = new HashMap<>(); // nodeId -> "IP:Port"
    private final List<DownloadRequest> downloadLogs = new ArrayList<>();
    private final Map<String, DownloadLease> activeDownloads = new HashMap<>(); // key: resourceName -> lease
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Condition> resourceConditions = new HashMap<>();

    private static class DownloadLease {
        private final String sourceNode;
        private final String requesterId;
        private final String resourceName;

        private DownloadLease(String sourceNode, String requesterId, String resourceName) {
            this.sourceNode = sourceNode;
            this.requesterId = requesterId;
            this.resourceName = resourceName;
        }
    }

    public void registerNode(String nodeId, String ip, int port) {
        lock.writeLock().lock();
        try {
            nodeAddresses.put(nodeId, ip + ":" + port);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unregisterNode(String nodeId) {
        lock.writeLock().lock();
        try {
            nodeAddresses.remove(nodeId);
            // README: "le rilevazioni non vengono eliminate dalla tabella dell'aggregatore
            // ma non saranno più accessibili" — do NOT remove from networkData.
            // getNetworkData() already filters by online nodes
            // (nodeAddresses::containsKey).
            // Clean up any active downloads involving this node
            Iterator<Map.Entry<String, DownloadLease>> iterator = activeDownloads.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, DownloadLease> entry = iterator.next();
                DownloadLease lease = entry.getValue();
                if (nodeId.equals(lease.requesterId) || nodeId.equals(lease.sourceNode)) {
                    downloadLogs
                            .add(new DownloadRequest(lease.sourceNode, lease.requesterId, lease.resourceName, false));
                    iterator.remove();

                    // Sveglia eventuali thread in attesa sulla risorsa
                    Condition condition = resourceConditions.get(lease.resourceName);
                    if (condition != null) {
                        condition.signalAll();
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addResource(String nodeId, String resource) {
        lock.writeLock().lock();
        try {
            networkData.computeIfAbsent(resource, k -> new TreeSet<>()).add(nodeId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<String> getOnlineNodes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(nodeAddresses.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<String> findOwners(String resource) {
        lock.readLock().lock();
        try {
            Set<String> owners = networkData.get(resource);
            if (owners == null) {
                return new ArrayList<>();
            }
            return owners.stream()
                    .filter(nodeAddresses::containsKey)
                    .sorted()
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    // Only show online nodes in the output (no "(offline)" suffix)
    public List<String> getNetworkData() {
        lock.readLock().lock();
        try {
            return networkData.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> {
                        String owners = entry.getValue().stream()
                                .filter(nodeAddresses::containsKey) // only online nodes
                                .sorted()
                                .collect(Collectors.joining(", "));
                        return "- " + entry.getKey() + ": " + owners;
                    })
                    .filter(line -> !line.endsWith(": ")) // skip resources with no online owners
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    private Condition getResourceCondition(String resource) {
        return resourceConditions.computeIfAbsent(resource, k -> lock.writeLock().newCondition());
    }

    public String requestToken(String resource, String requesterId) {
        lock.writeLock().lock();
        try {
            if (!networkData.containsKey(resource)) {
                downloadLogs.add(new DownloadRequest("unknown", requesterId, resource, false));
                return "NOT_FOUND";
            }

            String ownerId = findOnlineOwner(resource, requesterId);
            if (ownerId == null) {
                downloadLogs.add(new DownloadRequest("unknown", requesterId, resource, false));
                return "NO_OWNER";
            }

            Condition condition = getResourceCondition(resource);

            // Aspetta finché il token non è libero
            while (activeDownloads.containsKey(resource)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return "CONNECTION_ERROR";
                }
            }

            // Ri-verifica dopo l'attesa (la situazione potrebbe essere cambiata)
            if (!networkData.containsKey(resource)) {
                return "NOT_FOUND";
            }
            ownerId = findOnlineOwner(resource, requesterId);
            if (ownerId == null) {
                return "NO_OWNER";
            }

            activeDownloads.put(resource, new DownloadLease(ownerId, requesterId, resource));
            return ownerId + " " + nodeAddresses.get(ownerId);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<String> requestTokensForNode(String targetNode, String requesterId) {
        lock.writeLock().lock();
        try {
            List<String> results = new ArrayList<>();
            if (!nodeAddresses.containsKey(targetNode)) {
                return results;
            }

            List<String> targetResources = new ArrayList<>();
            for (Map.Entry<String, Set<String>> entry : networkData.entrySet()) {
                if (entry.getValue().contains(targetNode)) {
                    targetResources.add(entry.getKey());
                }
            }

            // Ordina per evitare deadlock quando si acquisiscono lock multipli
            Collections.sort(targetResources);

            for (String resource : targetResources) {
                Condition condition = getResourceCondition(resource);

                boolean interrupted = false;
                while (activeDownloads.containsKey(resource)) {
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        interrupted = true;
                        break;
                    }
                }

                if (interrupted) {
                    break;
                }

                // Riverifica che il nodo abbia ancora la risorsa dopo l'attesa
                Set<String> owners = networkData.get(resource);
                if (owners != null && owners.contains(targetNode) && nodeAddresses.containsKey(targetNode)) {
                    String ipPort = nodeAddresses.get(targetNode);
                    activeDownloads.put(resource, new DownloadLease(targetNode, requesterId, resource));
                    results.add(targetNode + " " + ipPort + " " + resource);
                }
            }

            return results;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String findOnlineOwner(String resource, String requesterId) {
        Set<String> nodes = networkData.get(resource);
        if (nodes == null) {
            return null;
        }

        // Prefer a node that is not the requester
        for (String nodeId : nodes) {
            if (!nodeId.equals(requesterId) && nodeAddresses.containsKey(nodeId)) {
                return nodeId;
            }
        }

        // Fallback: allow self-download if no other owner
        for (String nodeId : nodes) {
            if (nodeAddresses.containsKey(nodeId)) {
                return nodeId;
            }
        }

        return null;
    }

    public void releaseToken(String resource, String requesterId, boolean success) {
        lock.writeLock().lock();
        try {
            DownloadLease lease = activeDownloads.get(resource);
            if (lease != null && !lease.requesterId.equals(requesterId)) {
                return;
            }
            activeDownloads.remove(resource);
            if (lease != null) {
                downloadLogs.add(new DownloadRequest(lease.sourceNode, lease.requesterId, resource, success));

                if (!success) {
                    Set<String> owners = networkData.get(resource);
                    if (owners != null) {
                        owners.remove(lease.sourceNode);
                        if (owners.isEmpty()) {
                            networkData.remove(resource);
                        }
                    }
                }
            }

            // Sveglia i thread in attesa sul token di questa risorsa
            Condition condition = resourceConditions.get(resource);
            if (condition != null) {
                condition.signalAll();
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<DownloadRequest> getLogs() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(downloadLogs);
        } finally {
            lock.readLock().unlock();
        }
    }
}
