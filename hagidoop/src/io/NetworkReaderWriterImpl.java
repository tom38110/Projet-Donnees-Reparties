import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter {

    private Socket clientSocket;
    private ServerSocket serverSocket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private String host;
    private int port;


    /**
     * Constructeur pour le NetworkReaderWriterImpl
     * @param port
     * @param host
     */
    public NetworkReaderWriterImpl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Méthode pour envoyer un fragment au Reduce
     * @param clientSocket
     * @param fragmentFile
     * @throws IOException
     */
    @Override
    public KV read() {
        try {
            String ligne = reader.readLine(); // ligne du fichier/fragment
            if (ligne != null) {    // Tant que le fichier n'est pas vide
                String[] parties = ligne.split(KV.SEPARATOR); // stockage des KV dans la liste des parties
                return new KV(parties[0], parties[1]); // renvoie le couple kv 
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Méthode pour écrire KV record
     * @param record
     * @throws IOException
     */
    @Override
    public void write(KV record) {
        try {
            writer.write(record.k + KV.SEPARATOR + record.v); //ecris k<->v
            writer.newLine(); // on saute une ligne
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
            clientSocket = new Socket(host, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour accepter la connexion
     * @return
     */
    @Override
    public NetworkReaderWriter accept() {
        try {
            clientSocket = serverSocket.accept(); // Le Reduce attend qu'un Map se connecte 
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Méthode pour fermer le serveur
     */
    @Override
    public void closeServer() {
        try {
            //queue.put(null);
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
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
