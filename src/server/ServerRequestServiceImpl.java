package server;


import java.util.List;

import common.DownloadRequest;
import services.ServerRequestService;

public class ServerRequestServiceImpl implements ServerRequestService 
{
    private final ServerResource resource;

    public ServerRequestServiceImpl(ServerResource resource){
        this.resource=resource;
    }

    @Override
    public List<String> listData(){
        return resource.getNetworkData();
    }

    @Override
    public List<String> listNodes(){
        return resource.getOnlineNodes();
    }

    @Override
    public List<String> findOwners(String resourceName){
        return resource.findOwners(resourceName);
    }

    @Override
    public List<DownloadRequest> logData(){
        return resource.getLogs();
    }

    @Override
    public void registerNode(String nodeId,String ip,int port){
        resource.registerNode(nodeId, ip, port);
    }

    @Override
    public void unregisterNode(String nodeId){
        resource.unregisterNode(nodeId);
    }

    @Override
    public void addResource(String nodeId, String resourceName){
        resource.addResource(nodeId, resourceName);
    }

    @Override
    public String requestToken(String resourceName, String requesterId){
        String peerInfo=resource.requestToken(resourceName, requesterId);
        switch (peerInfo) 
        {
            case "NO_OWNER":
            case "NOT_FOUND":
                return peerInfo;
            default:
                return "SUCCESS " + peerInfo;
        }
    }

    @Override
    public List<String> requestTokensForNode(String nodeId, String requesterId){
        return resource.requestTokensForNode(nodeId, requesterId);
    }
    
    @Override
    public void releaseToken(String resourceName, String requesterId, boolean success)
    {
        resource.releaseToken(resourceName, requesterId, success);
    }
}
