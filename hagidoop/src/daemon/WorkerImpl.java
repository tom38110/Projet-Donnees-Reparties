package daemon;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    protected WorkerImpl() throws RemoteException {
        super();
    }

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        writer.openClient();
        reader.open("lecture");
        m.map(reader, writer);
        reader.close();
        writer.closeClient();
    }

    public static void main(String[] args) {
        try {
            int numWorker = Integer.parseInt(args[0]);
            Worker w = new WorkerImpl();
            try {
                Registry registry = LocateRegistry.createRegistry(8084);
            } catch (RemoteException e) {
                System.out.println("registry déjà créé");
            }
            Naming.rebind("//" + Project.hosts[numWorker] + ":8084/Worker" + numWorker, w);
            System.out.println("Worker " + numWorker + " bound in registry");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
}
