package hdfs;
import java.io.File;
import java.io.*;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
		try {
		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		if (fichier.delete()) {
			system.out.println("le fichier : " + fname + " a été effacé.");
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// fmt format du fichier (FMT_TXT ou FMT_KV)
	// lis chaque ligne du fichier sans le découper et stock les fragments dans les différents noeuds
	public static void HdfsWrite(int fmt, String fname) {
		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		FileReader lectFich = new FileReader(fichier);
		BufferedReader bufLect = new BufferedReader(lectFich);
		
		if (fmt == 0) { // FMT_TXT	
		
		} else if (fmt == 1) {// FMT_KV

		} else {

		}
	}

	public static void HdfsRead(String fname) {
		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande

	}
}
