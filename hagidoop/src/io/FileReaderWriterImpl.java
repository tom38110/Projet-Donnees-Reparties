package io;
import interfaces.FileReaderWriter;
import interfaces.KV;
import java.io.*;

public class FileReaderWriterImpl implements FileReaderWriter {
    
    private String fname;
    private BufferedReader reader; // modification avec ReaderImpl surement nécessaire
    private BufferedWriter writer; // modfication avec WriterImpl surement nécessaire
    private long indiceCourant;

    @Override
    public void write(KV record) {
        try {
            writer.write(record.k + KV.SEPARATOR + record.v); //ecris k<->v
            writer.newLine(); // on saute une ligne
        } catch (IOException e) {
            e.printStackTrace();
        }
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
    public void open(String mode) {
        try {
            // On choisit le mode (écriture ou lecture)
            if ("lecture".equals(mode)) { 
                reader = new BufferedReader(new FileReader(fname));
            } else if ("ecriture".equals(mode)) {
                writer = new BufferedWriter(new FileWriter(fname));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            if (reader != null) { // fermeture dans le cas d'un lecteur
                reader.close();
            }
            if (writer != null) { // fermeture dans le cas d'un rédacteur
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getIndex() {
        return indiceCourant;
    }

    @Override
    public String getFname() {
        return fname;
    }

    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }
}
