package services;

import java.util.List;

import common.DownloadRequest;

// interface for Server, to be implemented later
public interface ServerRequestService {
    List<String> listData();

    List<String> listNodes();

    List<DownloadRequest> logData();

    void registerNode(String nodeId, String ip, int port);

    void unregisterNode(String nodeId);

    void addResource(String nodeId, String resourceName);

    String requestToken(String resourceName, String requesterId);

    List<String> requestTokensForNode(String nodeId, String requesterId);

    void releaseToken(String resourceName, String requesterId, boolean success);
}
