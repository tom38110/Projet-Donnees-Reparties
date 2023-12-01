package io;
import interfaces.FileReaderWriter;
import interfaces.KV;

public class FileReaderWriterImpl implements FileReaderWriter {
    

    @Override
    public void write(KV record) {
        
    }

    @Override
    public KV read() {
        return 0;
    }

    @Override
    public void open(String mode) {
        // TODO
    }

    @Override
    public void close() {
        // TODO
    }

    @Override
    public long getIndex() {
        // TODO
    }

    @Override
    public String getFname() {
        // TODO
    }

    @Override
    public void setFname(String fname) {
        // TODO
    }
}
