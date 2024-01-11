package daemon;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.Map;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.KVFileReaderWriter;
import io.TxtFileReaderWriter;
import io.NetworkReaderWriterImpl;

public class JobLauncher {

	static class InnerJobLauncher implements Runnable {
		private Map m;
		private FileReaderWriter reader;
		private NetworkReaderWriter writer;
		private int numWorker;

		public InnerJobLauncher(Map m, FileReaderWriter reader, NetworkReaderWriter writer, int numWorker) {
			this.m = m;
			this.reader = reader;
			this.writer = writer;
			this.numWorker = numWorker;
		}

		// Lancement des runMap
		@Override
		public void run() {
			try {
				// Revoir port pour appli distribuée
				Worker worker = (Worker) Naming.lookup("//localhost:4000/Worker" + numWorker);
				worker.runMap(m, reader, writer);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void startJob (MapReduce mr, int format, String fname) {
		Set<Thread> threads = new HashSet<>();
		NetworkReaderWriter readerReduce = new NetworkReaderWriterImpl(Project.hostInit, Project.portInit, queue);
		// Ouverture du serveur (Reduce)
		readerReduce.openServer();
		FileReaderWriter writerReduce = new KVFileReaderWriter("res.txt", 0);
		BlockingQueue<KV> queue = new LinkedBlockingQueue<>();
		// Lancement des clients (Map)
		for (int i = 0; i < Project.nbNoeud; i++) {
			FileReaderWriter readerMap;
			NetworkReaderWriter writerMap;
			if (format == FileReaderWriter.FMT_TXT) {
				readerMap =  new TxtFileReaderWriter(fname, 0);
			} else {
				readerMap = new KVFileReaderWriter(fname, 0);
			}
			writerMap = new NetworkReaderWriterImpl(Project.hosts[i], Project.ports[i]);
			Thread t = new Thread(new InnerJobLauncher(mr, readerMap, writerMap, i));
			threads.add(t);
			t.start();
		}

		// Acceptation des connections demandées par les clients
		Set<NetworkReaderWriter> readersServer = new HashSet<>();
		for (int i = 0; i < Project.nbNoeud; i++) {
			NetworkReaderWriter readerServer = readerReduce.accept();
			readersServer.add(readerServer);
		}

		mr.reduce(readerReduce, writerReduce);

		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
