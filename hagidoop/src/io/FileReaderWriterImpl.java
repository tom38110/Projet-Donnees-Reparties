package io;
import interfaces.FileReaderWriter;
import interfaces.KV;
import java.io.*;

public class FileReaderWriterImpl implements FileReaderWriter {
    
    // Attributs
    private String fname;
    private BufferedReader reader; // modification avec ReaderImpl surement nécessaire
    private BufferedWriter writer; // modfication avec WriterImpl surement nécessaire
    private long indiceCourant;

    /**
     * Méthode pour écrire un KV record
     * @param record
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
     * Méthode pour lire un KV
     * @return un KV
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
     * Méthode pour ouvrir un fichier
     * @param mode mode d'ouverture du fichier (lecture ou écriture)
     */
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

    /**
     * Méthode pour fermer un fichier
     */
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

    /**
     * Méthode pour obtenir l'index courant
     * @return l'index courant
     */
    @Override
    public long getIndex() {
        return indiceCourant;
    }

    /**
     * Méthode pour obtenir le nom du fichier
     */
    @Override
    public String getFname() {
        return fname;
    }

    /**
     * Méthode pour définir le nom du fichier
     * @param fname
     */
    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }
}
