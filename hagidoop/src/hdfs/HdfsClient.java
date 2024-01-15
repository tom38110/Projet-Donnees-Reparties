package hdfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;

public class HdfsClient {
	private static final HashMap<Integer, String> serveurAdresses = new HashMap<>(); // indice , adresse 
	private static final HashMap<Integer, Integer> serveurPorts = new HashMap<>(); // indice, port
	
	private static void initServeurAdresses() {
        serveurAdresses.put(0, Project.hosts[0]);
        serveurAdresses.put(1, Project.hosts[1]);
        serveurAdresses.put(2, Project.hosts[2]);
    }

    private static void initServeurPorts() {
        serveurPorts.put(0, Project.ports[0]);
        serveurPorts.put(1, Project.ports[1]);
        serveurPorts.put(2, Project.ports[2]);
    }

	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	
	
	/* Probablement fonctionnel (en attente de test ) */
	// A refaire pour supprimer à distance...

	public static void HdfsDelete(String fname) {
		int ind = 0;
		for (int i = 0; i < Project.nbNoeud; i++) { 
			if (fname.equals("fragment_" + i + ".txt")) {
				ind = i;
				break;
			}
		}
		
		try {
			Socket socket = new Socket(serveurAdresses.get(ind), serveurPorts.get(ind));
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(socket.getOutputStream());

			// Envoi de la ligne au serveur
			oos.writeObject("supprimer");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static int nbLignes(String fname) {
		File fichier = new File(Project.PATH + "data/" +  fname);
		
		int j = 0;
		try  {
			FileReader lectFich = new FileReader(fichier);
			BufferedReader bufLect = new BufferedReader(lectFich);
			String ligne;

			while ((ligne = bufLect.readLine()) != null) {
				j ++;
			}

			bufLect.close();
		
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return j;
	}
	// fmt format du fichier (FMT_TXT ou FMT_KV)
	// lis chaque ligne du fichier sans le découper et stock les fragments dans les différents noeuds

	//Passer par les modulo pour traiter les lignes
	
	public static void HdfsWrite(int fmt, String fname) {
		int nbServ = Project.nbNoeud; // nombre de serveur = nombre de noeuds 
		File fichier = new File(Project.PATH + "data/" +  fname); // modifier le path avec celui de Hagimont
		int nbLigneFrag = nbLignes(fname)/ nbServ; // Nombre de ligne par fragment.
		if (fmt == 0 || fmt == 1 ) {
			try  {
				FileReader lectFich = new FileReader(fichier);
				BufferedReader bufLect = new BufferedReader(lectFich);

				for (int i= 0; i<nbServ; i++) {
					int debut =i*nbLigneFrag;
					int fin =(i+1)*nbLigneFrag;
					envoyerLigneAuServeur(bufLect, debut, fin, i); 
					//envoyerfmtAuServeur(fmt, i);
				}
				bufLect.close();

			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
	

	private static void envoyerLigneAuServeur(BufferedReader bufLect, int debut, int fin, int indiceServeur) {
		Integer port = serveurPorts.get(indiceServeur);
		String ligne;
		KV kv;
		if (port != null) {
			Socket socket = null;
			ObjectOutputStream oos = null;

			try {
				socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
				oos = new ObjectOutputStream(socket.getOutputStream());

				// Envoi de la ligne au serveur
				oos.writeObject("ecriture");
				for (int j = debut; j <= fin; j++) {
					ligne = bufLect.readLine(); // Condition normalement toujours vraie qui renvoie les lignes du fragment i
					kv = new KV(null, ligne);
					oos.writeObject(kv); // envoyer sous forme de KV utiliser constructeur de KV 
					//System.out.println(ligne); // peut être envoyer aussi le format ?? pour créer le fragment adéquat ??

				}
				ligne = null;
				kv = new KV(null, ligne);
				oos.writeObject(kv);
				// Fermeture du flux après l'envoi
				oos.close();

				// Vous pouvez ajouter ici la gestion de la réponse du serveur si nécessaire
				System.out.println("Ligne envoyée au serveur " + indiceServeur);

			} catch (IOException e) {
				e.printStackTrace();
			} 

		} else {
			System.out.println("Port non trouvé pour le serveur " + indiceServeur);
		}
	}

	public static void HdfsRead(String fname) {
		// Boucle sur tous les serveurs pour lire les fragments
		int ind = 0;
		for (int i = 0; i < Project.nbNoeud; i++) { 
			if (fname.equals("fragment_" + i + ".txt")) {
				ind = i;
				break;
			}
		}
			try {
				Socket socket = new Socket(serveurAdresses.get(ind) , serveurPorts.get(ind));
				ObjectOutputStream oos;
				oos = new ObjectOutputStream(socket.getOutputStream());

            	// Envoi de la ligne au serveur
				oos.writeObject("lecture");
				
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

				System.out.println("Fragment " + ind + ":");
				
				KV kv;
				try {
					while ((kv = (KV) ois.readObject()) != null) {
						System.out.println(kv.v);  // Afficher la ligne du fragment
						// Écrire la ligne dans le fichier, si nécessaire
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				oos.close();
				ois.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	

// AJOUTER UNE FONCTION QUI CHOISIT LE BON SERVEUR A PARTIR DU NOM DU FRAGMENT
	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		initServeurAdresses();
        initServeurPorts();

		String operation;
		String formatFichier = "txt";
		String fichierNom;
		if (args.length == 3 && args[0].equals("write")) {
			operation = args[0];
			formatFichier = args[1];
			fichierNom = args[2];
		} else if (args.length == 2 && (args[0].equals("read") || args[0].equals("delete"))) {
			operation = args[0];
			fichierNom = args[1];
		} else {
			System.out.println("Nombre d'arguments incorrects. \n");
			usage();
			return;
		}
		// Initialisation des variables nécessaires
		int fmt;

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
				break;
			case "delete" : // cas supprimer
				HdfsDelete(fichierNom); 
				break;
			
			default:
				System.out.println("Opération inconnue. \n");
				usage();
		}
	}
}

