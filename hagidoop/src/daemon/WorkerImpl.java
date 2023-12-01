package daemon;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private Map m;
    private FileReaderWriter reader;
    private NetworkReaderWriter writer;

    public WorkerImpl(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        this.m = m;
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        Worker worker = new WorkerImpl(m, reader, writer);
        
    }
    
}
