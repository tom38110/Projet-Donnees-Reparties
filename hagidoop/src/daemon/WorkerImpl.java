package daemon;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    private int nbWorker;

    protected WorkerImpl() throws RemoteException {
        super();
    }

    public WorkerImpl(int num) throws RemoteException {
        this.nbWorker = num;
    }

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        m.map(reader, writer);
    }

    public static void main(String[] args) {
        try {
            int numWorker = Integer.parseInt(args[0]);
            Worker w = new WorkerImpl(numWorker);
            // Revoir le port pour appli distribuée
            Registry registry = LocateRegistry.createRegistry(4000);
            Naming.rebind("//localhost:4000/Worker" + numWorker, w);
            System.out.println("Worker " + numWorker + " bound in registry");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
}
