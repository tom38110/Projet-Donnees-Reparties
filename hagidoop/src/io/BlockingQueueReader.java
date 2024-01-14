package io;

import java.util.concurrent.BlockingQueue;

import interfaces.KV;
import interfaces.Reader;

public class BlockingQueueReader implements Reader {

    private BlockingQueue<KV> queue;

    public BlockingQueueReader(BlockingQueue<KV> queue) {
        this.queue = queue;
    }

    @Override
    public KV read() {
        KV kv = null;
        try {
            kv = this.queue.take();
            if (kv.k == null && kv.v == null) {
                return null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return kv;
    }
    
}
