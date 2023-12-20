package io;

import java.net.ServerSocket;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter{

    ServerSocket ss = new ServerSocket();
    @Override
    public KV read() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'read'");
    }

    @Override
    public void write(KV record) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }

    @Override
    public void openServer() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'openServer'");
    }

    @Override
    public void openClient() {
        
    }

    @Override
    public NetworkReaderWriter accept() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'accept'");
    }

    @Override
    public void closeServer() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeServer'");
    }

    @Override
    public void closeClient() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeClient'");
    }
    
}
