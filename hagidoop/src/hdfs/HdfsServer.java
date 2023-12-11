package hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import config.Project;

public class HdfsServer {
    public static void main(String[] args) {
        try {
            int numero = Integer.parseInt(args[0]);
            ServerSocket ss = new ServerSocket(Project.ports[numero]);
            while (true) {
                Socket s = ss.accept();
                OutputStream os = s.getOutputStream();
                InputStream is = s.getInputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                ObjectInputStream ois = new ObjectInputStream(is);
                String ligneFrag;
                do {
                    ligneFrag = (String) ois.readObject();
                    System.out.println(ligneFrag);
                } while (ligneFrag != null);
                oos.close();
                ois.close();
                os.close();
                is.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
