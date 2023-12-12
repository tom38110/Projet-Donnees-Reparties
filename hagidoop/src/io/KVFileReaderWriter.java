package io;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class KVFileReaderWriter implements FileReaderWriter{

    /* Attributs */
    private String fname;
    private BufferedReader reader;
    private BufferedWriter writer;
    private long index;

    /**
     * Construire un KVFileReaderWriter à partir d'un nom d'un fichier KV et un indice de début
     * @param fileName le nom du fichier
     * @param index l'indice de départ
    */
    public KVFileReaderWriter(String fileName, long index){
        this.fname = fileName;
        this.index = index;
    }

    /* Méthodes */
    @Override
    public KV read() {
        String ligne = null;
        KV kv = null;
        try {
            ligne = reader.readLine();
            if (ligne != null) {
                String[] parties = ligne.split(KV.SEPARATOR); // Récupérer la clé et la valeur
                kv = new KV(parties[0], parties[1]);
                this.index++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } // Lire la ligne
        return kv;
    }

    @Override
    public void write(KV record) {
        try {
            writer.write(record.k + KV.SEPARATOR + record.v);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.index++;
    }

    @Override
    public void open(String mode) {
        try {
            // On choisit le mode (écriture ou lecture)
            if ("lecture".equals(mode)) { 
                reader = new BufferedReader(new FileReader(fname));
                this.index++;
            } else if ("ecriture".equals(mode)) {
                writer = new BufferedWriter(new FileWriter(fname));
                this.index++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
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
        return this.index;
    }

    @Override
    public String getFname() {
        return this.fname;
    }

    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }

}