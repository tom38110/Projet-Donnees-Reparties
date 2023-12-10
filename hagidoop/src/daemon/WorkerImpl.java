package daemon;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
                
    }
    
}
