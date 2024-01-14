package io;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import config.Project;
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


    /**
     * Constructeur pour le NetworkReaderWriterImpl
     * @param port
     * @param host
     * @param queue
     */
    public NetworkReaderWriterImpl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Constructeur pour le NetworkReaderWriterImpl
     * @param socket
     */
    public NetworkReaderWriterImpl(Socket socket) {
        this.socket = socket;
        try {
            is = socket.getInputStream();
            ois = new ObjectInputStream(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour lire un KV
     * @return 
     */
    @Override
    public KV read() {
        KV kv = null;
        try {
            kv = (KV) this.ois.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kv;
    }

    /**
     * Méthode pour écrire KV record
     * @param record
     */
    @Override
    public void write(KV record) {
        try {
            this.oos.writeObject(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            // Demande de connexion au serveur
            socket = new Socket(Project.hostInit, Project.portInit);
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
        } catch (IOException e) {
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
