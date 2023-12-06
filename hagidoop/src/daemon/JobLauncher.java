package daemon;

import java.rmi.RemoteException;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;

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
		for (int i = 0; i < Project.nbNoeud; i++) {
			Thread t = new Thread(new );

		}
	}
}
