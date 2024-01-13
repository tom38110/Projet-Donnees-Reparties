package io;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class Linker implements Runnable {

    private NetworkReaderWriter readerServeur;
    private BlockingQueue<KV> bq;

    public Linker(NetworkReaderWriter readerServeur, BlockingQueue<KV> bq) {
        this.readerServeur = readerServeur;
        this.bq = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        NetworkReaderWriter reader = readerServeur.accept();
        KV kv;
        do {
            kv = reader.read();
            try {
                bq.put(kv);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while(kv != null);
    }
}
