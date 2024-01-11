package io;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.Linker;
import interfaces.NetworkReaderWriter;

public class LinkerImpl implements Linker {

    private Set<NetworkReaderWriter> readers;
    private BlockingQueue<KV> bq;

    public LinkerImpl(Set<NetworkReaderWriter> readers) {
        this.readers = readers;
        this.bq = new LinkedBlockingQueue<>();
    }

    public NetworkReaderWriter fusion() {

    }

    @Override
    public KV read() {
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    @Override
    public void write(KV record) {
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }
}
