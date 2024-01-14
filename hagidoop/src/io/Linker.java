package io;

import java.util.concurrent.BlockingQueue;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class Linker implements Runnable {

    private NetworkReaderWriter readerServeur;
    private BlockingQueue<KV> bq;

    public Linker(NetworkReaderWriter readerServeur, BlockingQueue<KV> bq) {
        this.readerServeur = readerServeur;
        this.bq = bq;
    }

    @Override
    public void run() {
        NetworkReaderWriter reader = readerServeur.accept();
        KV kv;
        do {
            kv = reader.read();
            try {
                if (kv != null) {
                    bq.put(kv);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while(kv != null);
        try {
            bq.put(new KV(null, null));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
