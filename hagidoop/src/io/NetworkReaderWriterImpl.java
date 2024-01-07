import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter {

    private BlockingQueue<KV> queue; // Utilisation d'une BlockingQueue pour gérer la communication asynchrone entre les Map et le Reducer
    private Socket clientSocket; // Socket pour la connexion côté client (Map)
    private ServerSocket serverSocket; // ServerSocket pour accepter les connexions côté serveur (Reducer)
    private BufferedReader reader; // BufferedReader pour lire les données depuis la connexion
    private BufferedWriter writer; // BufferedWriter pour écrire les données vers la connexion

    public NetworkReaderWriterImpl() {
        this.queue = new LinkedBlockingQueue<>(); // Initialisation de la BlockingQueue
    }

    @Override
    public KV read() {
        try {
            if (reader != null) {
                String line = reader.readLine();
                if (line != null) {
                    String[] parts = line.split(KV.SEPARATOR);
                    return new KV(parts[0], parts[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(KV record) {
        try {
            queue.put(record); // Ajout de l'enregistrement à la BlockingQueue
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void openServer() {
        try {
            serverSocket = new ServerSocket(0); // Création d'un ServerSocket sur un port disponible (0)
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void openClient() {
        // Pas d'implémentation nécessaire ici pour le côté client
    }

    @Override
    public NetworkReaderWriter accept() {
        try {
            clientSocket = serverSocket.accept(); // Accepter la connexion côté serveur (Reducer)
            writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            // Lancer un thread pour gérer l'écriture dans le Reducer depuis la queue
            Thread reducerWriterThread = new Thread(() -> {
                try {
                    while (true) {
                        KV record = queue.take(); // Récupération d'un enregistrement depuis la BlockingQueue
                        if (record == null) {
                            break; // Marque la fin de l'écriture
                        }
                        writer.write(record.k + KV.SEPARATOR + record.v);
                        writer.newLine();
                        writer.flush();
                    }
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        // Fermer la connexion côté serveur une fois l'écriture terminée
                        writer.close();
                        reader.close();
                        clientSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            reducerWriterThread.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public void closeServer() {
        try {
            // Marquer la fin de l'écriture en ajoutant null à la BlockingQueue
            queue.put(null);
            serverSocket.close(); // Fermer le ServerSocket côté serveur
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closeClient() {
        // Pas d'implémentation nécessaire ici pour le côté client
    }
}
