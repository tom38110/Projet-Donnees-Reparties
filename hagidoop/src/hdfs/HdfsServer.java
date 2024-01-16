package hdfs;

import config.Project;
import interfaces.KV;

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
        if (args.length < 1) {
            System.out.println("Usage: java HdfsServer <NumeroPort>");
            return;
        }
        int fragmentIndex = Integer.parseInt(args[0]);
        try {
            ServerSocket serverSocket = new ServerSocket(Project.ports[fragmentIndex]);
            String fragName = "fragment_" + fragmentIndex + ".txt";
            File fragmentFile = new File(Project.PATH + "data/" + fragName);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
                ObjectOutputStream oos = new ObjectOutputStream(clientSocket.getOutputStream());
                
                try {
                    // On lit le mode demandé
                    String ligne = (String) ois.readObject();
                    KV kv;
                    if (ligne.equals("ecriture")) {
                        fragmentFile.createNewFile();
                        FileWriter fragmentWriter = new FileWriter(fragmentFile);
                        do {
                            kv = (KV) ois.readObject();
                            if (kv.v != null) {
                                fragmentWriter.write(kv.v);
                                fragmentWriter.write(System.lineSeparator());
                                //System.out.println(kv.v);
                            }
                        } while(kv.v != null);
                        fragmentWriter.close();
                        kv = new KV(null, "ok");
                        oos.writeObject(kv);
                    } else if (ligne.equals("lecture")) {
                         try (BufferedReader fragmentReader = new BufferedReader(new FileReader(fragmentFile));) {
                            while ((ligne = fragmentReader.readLine()) != null) {
                                kv = new KV(null, ligne);
                                oos.writeObject(kv);
                            }
                        } catch (FileNotFoundException e) {
                            // Gérer le cas où le fragment n'existe pas encore
                            System.out.println("Le fragment n'existe pas encore.");
                        }
                        // Signal de fin du fragment
                        oos.writeObject(null);

                    }  else if (ligne.equals("supprimer")) {
                        try{
                            File fichier = new File(Project.PATH + "data/" + fragName); 
                            if (fichier.delete()) {
                                System.out.println("le fichier : " + fragName + " a été effacé.");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}