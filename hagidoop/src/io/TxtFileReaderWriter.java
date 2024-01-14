package io;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;

/**
 * Lecture et écriture de KV dans un fichier texte. La clé K n'est pas utilisée
 * , elle vaudra null. La valeur est la ligne du fichier.
 */
public class TxtFileReaderWriter implements FileReaderWriter {

    private long index;
    private String fname;
    private BufferedReader reader;
    private BufferedWriter writer;

    /**
     * Construit un TxtFileReaderWriter à partir d'un nom de fichier au format
     * txt et d'un index de début.
     * @param fname le nom du fichier traité
     * @param index l'index de la ligne début
     */
    public TxtFileReaderWriter(String fname, long index) {
        this.index = index;
        this.fname = Project.PATH + "data/" + fname;
    }



    @Override
    public KV read() {
        String ligne = null;
        KV kv = null;
        try {
            if (reader != null) {
                ligne = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (ligne != null) {
            kv = new KV(null, ligne);
            this.index++;
        }
        return kv;
    }

    @Override
    public void write(KV record) {
        try {
            if (record != null && record.k != null) {
                writer.write(record.k + KV.SEPARATOR + record.v);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.index++;
    }

    @Override
    public void open(String mode) {
        try {
            if (mode.equals("lecture")) {
                reader = new BufferedReader(new FileReader(this.fname));
            } else if (mode.equals("ecriture")) {
                writer = new BufferedWriter(new FileWriter(this.fname));
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
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
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