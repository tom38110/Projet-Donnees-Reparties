import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter {

    /**
     * Attributs de la classe NetworkReaderWriterImpl
     */
    private Socket socket;
    private ServerSocket serverSocket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    private InputStream is;
    private OutputStream os;
    private String host;
    private int port;
    private BlockingQueue<KV> queue;


    /**
     * Constructeur pour le NetworkReaderWriterImpl
     * @param port
     * @param host
     * @param queue
     */
    public NetworkReaderWriterImpl(String host, int port, BlockingQueue<KV> queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    /**
     * Constructeur pour le NetworkReaderWriterImpl
     * @param socket
     */
    public NetworkReaderWriterImpl(Socket socket) {
        this.socket = socket;
        is = socket.getInputStream();
        ois = new ObjectInputStream(is);
    }

    /**
     * Méthode pour lire un KV
     * @return 
     */
    @Override
    public KV read() {
        return (KV) this.ois.readObject();
    }

    /**
     * Méthode pour écrire KV record
     * @param record
     */
    @Override
    public void write(KV record) {
        this.oos.writeObject(record);
    }

    /**
     * Méthode pour ouvrir le serveur (Reduce)
     */
    @Override
    public void openServer() {
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour ouvrir le client (Map)
     */
    @Override
    public void openClient() {
        try {
            socket = new Socket(host, port);
            os = socket.getOutputStream();
            oos = new ObjectOutputStream(os);
        } catch (IOException e) {    
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour accepter la connexion
     * @return nouveau NetworkReaderWriterImpl
     */
    @Override
    public NetworkReaderWriter accept() {
        try {
            socket = serverSocket.accept(); // Le Reduce attend qu'un Map se connecte 
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new NetworkReaderWriterImpl(socket);
    }

    /**
     * Méthode pour fermer le serveur
     */
    @Override
    public void closeServer() {
        try {
            ois.close();
            is.close();
            socket.close();
            serverSocket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour fermer le client
     */
    @Override
    public void closeClient() {
        try {
            oos.close();
            os.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
