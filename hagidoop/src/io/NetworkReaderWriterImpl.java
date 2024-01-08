import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter {

    private BlockingQueue<KV> queue; // Pour gérer la communication entre les Map et le Reducer
    private Socket clientSocket;
    private ServerSocket serverSocket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public NetworkReaderWriterImpl() {
        this.queue = new LinkedBlockingQueue<>();
    }

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

    @Override
    public void write(KV record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void openServer() {
        try {
            serverSocket = new ServerSocket(); // *********** Je sais pas quel port je dois mettre
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void openClient() {
        try {
            clientSocket = new Socket();// *********** Je sais pas quel port je dois mettre
            writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Méthode pour accepter une connexion depuis un Map
     */
    @Override
    public NetworkReaderWriter accept() {
        try {
            clientSocket = serverSocket.accept(); // Le Reduce attend qu'un Map se connecte 
            writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream())); // Pour écrire dans le Reduce
            reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); // Pour lire les données qui viennent depuis les Maps

            // Un thread pour gérer l'écriture dans le Reduce
            Thread reducerWriterThread = new Thread(() -> {
                try {
                    while (true) {
                        KV record = queue.take(); // Récupération les données depuis la queue
                        if (record == null) {
                            break; // Fin d'écriture
                        }
                        // Ecrire dans le reduce
                        writer.write(record.k + KV.SEPARATOR + record.v); // Ecrire la donnée qui vient depuis la queue sous forme d'un KV
                        writer.newLine(); // Retourne à la ligne
                        writer.flush(); // Pour s'assurer que toutes les données sont écrites immédiatement dans la sortie, comme ça on n'attend pas que le tampon (le buffer) se remplit avant d'envoyé
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        // Fermeture de la connexion
                        writer.close();
                        reader.close();
                        clientSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            // Lancer le Thread
            reducerWriterThread.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public void closeServer() {
        try {
            // Marquer la fin de l'écriture en ajoutant null à la fin de la queue
            queue.put(null);
            serverSocket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeClient() {
        try {
            clientSocket.close();
            writer.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
