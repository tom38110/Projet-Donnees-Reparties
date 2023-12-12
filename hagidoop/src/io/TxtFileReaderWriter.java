import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

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
        this.fname = fname;
    }

    @Override
    public KV read() {
        String ligne = reader.readLine();
        KV kv = new KV(null, ligne);
        this.index++;
        return kv;
    }

    @Override
    public void write(KV record) {
        writer.write(record.k + KV.SEPARATOR + record.v); 
        writer.newLine();
        this.index++;       
    }

    @Override
    public void open(String mode) {
        if (mode.equals("lecture")) {
            reader = new BufferedReader(new FileReader(this.fname));
        } else if (mode.equals("ecriture")) {
            writer = new BufferedWriter(new FileWriter(this.fname));
        }
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
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