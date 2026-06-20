package services;

import java.util.List;

import common.DownloadInfo;
import common.Rilevazione;

// this class is only an interface for the later operations we will do
public interface ClientRequestService {
    List<Rilevazione> listLocalData(String nodeId);

    List<String> listNodes();

    Rilevazione add(String nodeId, Rilevazione rilevazione);

    List<String> listRemoteData();

    DownloadInfo download(String resourceName, String localNodeId);

    DownloadInfo downloadFromNode(String nodeId, String localNodeId);

    String connectToAggregator(String host, int port, String nodeId, int peerPort) throws Exception;

    void disconnect(String nodeId);
}
