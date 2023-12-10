package hdfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.Reader;
import interfaces.Writer;
import io.FileReaderWriterImpl;

public class HdfsClient {
	private static final HashMap<Integer, String> serveurAdresses = new HashMap<>(); // indice , adresse
	private static final HashMap<Integer, Integer> serveurPorts = new HashMap<>(); // indice, port
	
	
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
			System.out.println("le fichier : " + fname + " a été effacé.");
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	// fmt format du fichier (FMT_TXT ou FMT_KV)
	// lis chaque ligne du fichier sans le découper et stock les fragments dans les différents noeuds
	public static void HdfsWrite(int fmt, String fname) {

		int nbServ = Project.nbNoeud; // nombre de serveur = nombre de noeuds ? (pas sur de cette interprétation)
		
		ArrayList<String> lignes = new ArrayList<>(); // liste des lignes stockées 

		int nbLigneFrag = lignes.size()/ nbServ; // Nombre de ligne par fragment.

		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont

		// On va recupérer les lignes dans la liste
        try  {
			FileReader lectFich = new FileReader(fichier);
			BufferedReader bufLect = new BufferedReader(lectFich);

            String ligne;
            while ((ligne = bufLect.readLine()) != null) {
                lignes.add(ligne);
            }
			bufLect.close();

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
				String fragName = "fragment" + fname + "-" + i;
				envoyerFragmentAuServeur(fragName, i);	// envoyer les fragments vers les serveurs...
			}

		} else if (fmt == 1) {// FMT_KV
			// 
			// Création de la liste des KV
			HashMap<String,Integer> listeKV = new HashMap<String,Integer>();
			KV kv;
			//try{
			//FileReader lectFich = new FileReader(fichier);
			// création du reader
			//BufferedReader lecteur = new BufferedReader(lectFich);
			//} catch(IOException e) {
			//	e.printStackTrace();	
			//}

			FileReaderWriter readerWriter = getReaderWriter(FileReaderWriter.FMT_TXT, fname);
			readerWriter.open("write");
			HashMap<Integer,String> listeCle = new HashMap<Integer,String>();
			int ind = 0;
			// Recupérer la liste des couples KV 
			while ((kv = readerWriter.read()) != null) {
				String tokens[] = kv.v.split(" ");
				for (String tok : tokens) {
					listeKV.put(tok, 1);
					listeCle.put(ind, tok);
					ind ++;
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
							int val = listeKV.get(listeCle.get(j)); // On récupère la valeur associé à la bonne clé
							bufEcr.write(val);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				String fragName = "fragment" + fname + "-" + i;
				envoyerFragmentAuServeur(fragName, i);	// envoyer les fragments vers les serveurs...
			}
		}
	}



	private static void envoyerFragmentAuServeur(String fragName, int indiceServeur) {
		try (Socket socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
				oos.writeObject("ecriture");
				oos.writeObject(fragName);

				try (BufferedReader bufLectFrag = new BufferedReader(new FileReader("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" + fragName))) {
					String ligneFrag;
					while ((ligneFrag = bufLectFrag.readLine()) != null ){
						oos.writeObject(ligneFrag);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				oos.writeObject(null);
				//Envoyer un message comme quoi le fragment a été correctement envoyé

				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				String reponse = (String) ois.readObject();

				System.out.println(reponse);

			} catch (IOException | ClassNotFoundException e ){
				e.printStackTrace();
			}

	}



	
	private static FileReaderWriter getReaderWriter(int fmt, String fname) {
        // renvoie le FileReaderWriter au bon format.
		if (fmt == FileReaderWriter.FMT_TXT) {
			return new FileReaderWriterImpl();
		} else if (fmt == FileReaderWriter.FMT_KV) {
			return new FileReaderWriterImpl();
			// return new AJOUTER UN CONSTRUCTEUR POUR LES KV
		} else { 
			return null;
		}
	}




	public static void HdfsRead(String fname) {
		File fichier = new File("/mnt/c/Users/yanis/Documents/GitHub/Projet-Donnees-Reparties/" 
								+ fname); // modifier le path avec celui de Hagimont
		try {		
			// Obtention du bon FileReaderWriter au format texte
			FileReaderWriter readerWriter = getReaderWriter(FileReaderWriter.FMT_TXT, fname);
			readerWriter.open("lecture");
			// permet de lire le fichier

			FileWriter lectFich = new FileWriter(fichier);
			
			// On va ouvrir un Buffered Writer pour stocker les fragments lus
			BufferedWriter bufEcr = new BufferedWriter(lectFich);
			KV kv;
			
			while ((kv = readerWriter.read()) != null) {
				//Ecrire la ligne dans Hdfs 
				bufEcr.write(kv.v);
				bufEcr.newLine();
			}
			
			// Fermeture des ressources
			readerWriter.close();
			bufEcr.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
}




	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		if (args.length < 2) {
			System.out.println("Nombre d'arguments incorrects. \n");
			usage();
			return;
		}
		// Initialisation des variables nécessaires
		int fmt;
		String operation = args[0];
		String formatFichier = args[1];
		String fichierNom = args [2];

		switch (operation) {
			case "read" : //cas écriture
				HdfsRead(fichierNom);
				break;
			case "write" : // cas lecture
				if (formatFichier.equals("txt")) { // on donne le format txt
					fmt = FileReaderWriter.FMT_TXT;
					HdfsWrite(fmt, fichierNom);
				} else if(formatFichier.equals("kv")) { // on donne le format kv
					fmt =FileReaderWriter.FMT_KV;
					HdfsWrite(fmt, fichierNom);
				} 
			case "delete" : // cas supprimer
				HdfsDelete(fichierNom); 
				break;
			default:
				System.out.println("Opération inconnue. \n");
				usage();
		}
	}
}

