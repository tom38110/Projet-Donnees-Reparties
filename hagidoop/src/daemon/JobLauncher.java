package daemon;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.FileReaderWriterImpl;

public class JobLauncher {

	static class InnerJobLauncher implements Runnable {
		private Map m;
		private FileReaderWriter reader;
		private NetworkReaderWriter writer;

		public InnerJobLauncher(Map m, FileReaderWriter reader, NetworkReaderWriter writer) {
			this.m = m;
			this.reader = reader;
			this.writer = writer;
		}

		// Lancement des runMap
		@Override
		public void run() {
			try {
				Worker worker = new WorkerImpl();
				worker.runMap(m, reader, writer);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}

	public static void startJob (MapReduce mr, int format, String fname) {
		Set<Thread> threads = new HashSet<>();
		for (int i = 0; i < Project.nbNoeud; i++) {
			FileReaderWriter reader =  new FileReaderWriterImpl();
			NetworkReaderWriter writer = new NetworkReaderWriterImpl();
			Thread t = new Thread(new InnerJobLauncher(mr, reader, writer));
			threads.add(t);
		}

		for (Thread t : threads) {
			t.join();
		}
	}
}
