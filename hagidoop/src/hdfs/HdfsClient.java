package hdfs;
import java.io.File;
import java.io.*;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	/* Probablement fonctionnel (en attente de test ) */
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
	
	/* public static int nbLigne (String fname) {
		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		FileReader lectFich = new FileReader(fichier);
		BufferedReader bufLect = new BufferedReader(lectFich);
		int nb = 0;
		while ()
		if (bufLect == "\n") {
			nb ++;
		}
	} */


	// fmt format du fichier (FMT_TXT ou FMT_KV)
	// lis chaque ligne du fichier sans le découper et stock les fragments dans les différents noeuds
	public static void HdfsWrite(int fmt, String fname) {
		int nbServ = 3 // nombre de serveur = nombre de noeuds (à généraliser)
		
		int nbLigneFrag = lignes.size()/ nbServ; // Nombre de ligne par fragment.

		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		
		FileReader lectFich = new FileReader(fichier); // permet de lire le fichier
		
		List<String> lignes = new ArrayList<>(); // liste des lignes stockées 

		// On va recupérer les lignes dans la liste
        try (BufferedReader bufLect = new BufferedReader(lectFich)) {
            String ligne;
            while ((ligne = bufLect.readLine()) != null) {
                lignes.add(ligne);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

		if (fmt == 0) { // FMT_TXT	
			
			for (int i= 0; i<nbServ; i++) {
				// stocker les fragments dans une liste ?? 

				// indices de début et de fin des fragments
				int debut =i*nbLigneFrag;
				int fin =(i+1)*nbLigneFrag;

				// création des i fragments en .txt
				try {
					File fragment = new File("fragment" + fname + "-" + i);
		
					// Vérifier si le fichier existe déjà.
					if (fragment.createNewFile()) {
						System.out.println("Le fragment" + i + " a été créé !");
					} else {
						System.out.println("Le fragment" + i + " existe déjà.");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				//Ajouter le nombre de lignes adéquates dans chaque fragments.
				try (BufferedWriter bufEcr = new BufferedWriter(new FileWriter(fname))) {
					for (int j = debut; j <= fin; j++) {
						if (j >= 0 && j < lignes.size()) {
							bufEcr.write(lignes.get(j));
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				envoyerFragmentAuServeur(fragment, i); // envoyer les fragments vers les serveurs...
			}

		} else if (fmt == 1) {// FMT_KV
			// 
			// Création de la liste des KV
			HashMap<String,Integer> listeKV = new HashMap<String,Integer>();
			KV kv;
			// création du reader
			Reader lecteur = new Reader(lectFich);

			// Recupérer la liste des couples KV 
			while ((kv = lecteur.read()) != null) {
				String tokens[] = kv.v.split(" ");
				for (String tok : tokens) {
					listeKV.put(tok, 1);
				}
			}

			for (int i= 0; i<nbServ; i++) {
				// indices de début et de fin des fragments
				int debut =i*nbLigneFrag;
				int fin =(i+1)*nbLigneFrag;

				// création des i fragments en .txt
				try {
					File fragment = new File("fragment" + fname + "-" + i);
		
					// Vérifier si le fichier existe déjà.
					if (fragment.createNewFile()) {
						System.out.println("Le fragment" + i + " a été créé !");
					} else {
						System.out.println("Le fragment" + i + " existe déjà.");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				//Ajouter le nombre de couple KV adéquates dans chaque fragments.
				try (BufferedWriter bufEcr = new BufferedWriter(new FileWriter(fname))) {
					for (int j = debut; j <= fin; j++) {
						if (j >= 0 && j < listeKV.size()) {
							bufEcr.write(ToString (listeKV.get(j)));
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				envoyerFragmentAuServeur(fragment, i);	// envoyer les fragments vers les serveurs...
			}
		}
	}

	public static void envoyerFragmentAuServeur(String frag, int indiceServeur) {
		// coder l'envoie serveur ALED TOM
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
