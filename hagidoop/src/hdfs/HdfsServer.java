package hdfs;

import config.Project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class HdfsServer {
    public static void main(String[] args) {  
        /*if (args.length < 1) {
            System.out.println("Usage: java HdfsServer <fragment_index>");
            return;
        }

        int fragmentIndex = Integer.parseInt(args[0]);

        try {
            ServerSocket serverSocket = new ServerSocket(Project.ports[fragmentIndex]);
            String fragName = "fragment-" + fragmentIndex;
            File fragmentFile = new File(Project.PATH + "data/" + fragName);
            fragmentFile.createNewFile();
            boolean cond =true;
            while (true) {
                Socket clientSocket = serverSocket.accept();
                ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());

                // Partie qui gère la lecture et la création des fragments
                try {
                    String requete = (String) ois.readObject();
                    if (requete!=null) {
                        if (requete.equals("ecriture")) {
                            // Le client envoie des lignes pour écrire dans le fragment
                            creerFragment(ois, fragmentFile);
                        } else if (requete.equals("lecture")) {
                            // Le client demande la lecture d'un fragment
                            envoieFragmentToClient(clientSocket, fragmentFile);
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    ois.close();
                    clientSocket.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

         
        if (args.length < 1) {
            System.out.println("Usage: java HdfsServer <NumeroPort>");
            return;
        }
        int fragmentIndex = Integer.parseInt(args[0]);
        try {
            ServerSocket serverSocket = new ServerSocket(Project.ports[fragmentIndex]);
            String fragName = "fragment-" + fragmentIndex;
            File fragmentFile = new File(Project.PATH + "data/" + fragName);
            fragmentFile.createNewFile();
            FileWriter fragmentWriter = new FileWriter(fragmentFile);
            boolean cond = true;
            while (true) {
                while (cond) {
                    Socket clientSocket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
                    ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                    
                    try {
                        String ligne = (String) ois.readObject();
                        if (ligne != null) {
                            fragmentWriter.write(ligne);
                            fragmentWriter.write(System.lineSeparator());
                            System.out.println(ligne);
                        } else {
                            cond = false;
                        }
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } finally {
                        ois.close();
                        oos.close();
                        clientSocket.close();
                    }
                }
                fragmentWriter.close();
                cond = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    } 
    
    /*
    /* Lecture des lignes entrante et Traitement de l'écriture sur le fragment créé 
    private static void creerFragment(ObjectInputStream ois, File fragmentFile) {
        
        try (FileWriter fragmentWriter = new FileWriter(fragmentFile)) {
            
            String ligneRecue = (String) ois.readObject();
            // Tant qu'on est pas arrivé à la fin du fragment on écrit la ligne.
            while (ligneRecue  != null) {                       
                String ligne = (String) ligneRecue;
                fragmentWriter.write(ligne);
                fragmentWriter.write(System.lineSeparator());
                System.out.println(ligne);
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /* Envoie des fragments pourqu'ils soientt lisibles par le client 
    private static void envoieFragmentToClient(Socket clientSocket, File fragmentFile) throws IOException {
        try (BufferedReader fragmentReader = new BufferedReader(new FileReader(fragmentFile));
             ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream())) {
            String ligne;
            while ((ligne = fragmentReader.readLine()) != null) {
                oos.writeObject(ligne);
            }
        } catch (FileNotFoundException e) {
            // Gérer le cas où le fragment n'existe pas encore
            System.out.println("Le fragment n'existe pas encore.");
        }

        // Signal de fin du fragment
        try (ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream())) {
            oos.writeObject(null);
        }
    }*/

}