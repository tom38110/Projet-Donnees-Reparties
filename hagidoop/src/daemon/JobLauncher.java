package daemon;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.Map;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.FileReaderWriterImpl;
import io.KVFileReaderWriter;
import io.TxtFileReaderWriter;

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
		FileReaderWriter readerMap;
		for (int i = 0; i < Project.nbNoeud; i++) {
			if (format == FileReaderWriter.FMT_TXT) {
				readerMap =  new TxtFileReaderWriter(fname, 0);
			} else {
				readerMap = new KVFileReaderWriter(fname, 0);
			}
			NetworkReaderWriter writerMap = new NetworkReaderWriterImpl();
			Thread t = new Thread(new InnerJobLauncher(mr, readerMap, writerMap));
			threads.add(t);
		}

		for (Thread t : threads) {
			t.join();
		}

		NetworkReaderWriter readerReduce = new NetworkReaderWriterImpl();
		FileReaderWriter writerReduce = new TxtFileReaderWriter(fname, 0);
		mr.reduce(readerReduce, writerReduce);
	}
}
