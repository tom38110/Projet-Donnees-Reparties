package hdfs;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;
import io.FileReaderWriterImpl;
import io.TxtFileReaderWriter;
import io.KVFileReaderWriter;
import io.TxtFileReaderWriter;

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
		try {
		File fichier = new File(Project.PATH + "data/" + fname); // modifier le path avec celui de Hagimont
		if (fichier.delete()) {
			System.out.println("le fichier : " + fname + " a été effacé.");
		}
		} catch (Exception e) {
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
				String ligne;
				int j = 0;

				for (int i= 0; i<nbServ; i++) {
					int debut =i*nbLigneFrag;
					int fin =(i+1)*nbLigneFrag;
					for (j = debut; j <= fin; j++) {
						ligne = bufLect.readLine(); // Condition normalement toujours vraie qui renvoie les lignes du fragment i
						envoyerLigneAuServeur(ligne, i);
						System.out.println(ligne); // peut être envoyer aussi le format ?? pour créer le fragment adéquat ??

					}
					j++;
					envoyerLigneAuServeur(null, i);
					//envoyerfmtAuServeur(fmt, i);
				}
				bufLect.close();

			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
	

	private static void envoyerLigneAuServeur(String ligne, int indiceServeur) {
    Integer port = serveurPorts.get(indiceServeur);

    if (port != null) {
        Socket socket = null;
        ObjectOutputStream oos = null;

        try {
            socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
            oos = new ObjectOutputStream(socket.getOutputStream());

            // Envoi de la ligne au serveur
            oos.writeObject(ligne); // envoyer sous forme de KV utiliser constructeur de KV 

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
	
	/*
	private static void envoyerfmtAuServeur(int fmt, int indiceServeur) {
        Integer port = serveurPorts.get(indiceServeur);

        if (port != null) {
            try (Socket socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
                 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

                // Envoi du type d'opération (si nécessaire)
                // oos.writeObject("operation"); 

                // Envoi du format au serveur
                oos.writeObject(fmt);
				oos.writeObject(indiceServeur);
				
				System.out.println("Fin envoie fragment "+ indiceServeur);
				oos.writeObject(null);
                // Fermeture de la connexion
                oos.close();

                // Vous pouvez ajouter ici la gestion de la réponse du serveur si nécessaire

                System.out.println("Format envoyée au serveur " + indiceServeur);

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Port non trouvé pour le serveur " + indiceServeur);
        }
    }	
*/

	public static void HdfsRead(String fname) {
		// Boucle sur tous les serveurs pour lire les fragments
		for (int i = 0; i < Project.nbNoeud; i++) { // à enlever
			try {
				Socket socket = new Socket(serveurAdresses.get(i), serveurPorts.get(i));
				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

				System.out.println("Fragment " + i + ":");
				
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);  // Afficher la ligne du fragment
					// Écrire la ligne dans le fichier, si nécessaire
				}

				reader.close();
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/*public static void HdfsRead(String fname, int indiceServeur) {
		try {
			Socket socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
	
			try {
				String line;
				while ((line = (String) ois.readObject()) != null) {
					if (line.equals("fin_fragment")) {
						break; // Sortir de la boucle lorsque le marqueur de fin est reçu
					}
					System.out.println(line);  // Afficher la ligne du fragment
					// Écrire la ligne dans le fichier, si nécessaire
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				ois.close();
				socket.close();
				System.out.println("Fragment " + indiceServeur + ": Received all lines from server");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
		/*
		File fichier = new File(Project.PATH + "data/" + fname); // modifier le path avec celui de Hagimont
		try {
			// Obtention du bon FileReaderWriter au format texte
			TxtFileReaderWriter readerWriter = new TxtFileReaderWriter(fname,1);
			readerWriter.open("lecture");
			// permet de lire le fichier

			FileWriter lectFich = new FileWriter(fichier);
			
			// On va ouvrir un Buffered Writer pour stocker les fragments lus
			BufferedWriter bufEcr = new BufferedWriter(lectFich);
			KV kv;
			
			while ((kv = readerWriter.read()) != null) {
				//Ecrire la ligne dans Hdfs 
				bufEcr.write(kv.toString());
				bufEcr.newLine();
			}
			
			// Fermeture des ressources
			readerWriter.close();
			bufEcr.close();

		} catch (IOException e) {
			e.printStackTrace();
		}*/
	

// AJOUTER UNE FONCTION QUI CHOISIT LE BON SERVEUR A PARTIR DU NOM DU FRAGMENT
	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande
		initServeurAdresses();
        initServeurPorts();

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
				envoyerLigneAuServeur("lecture", 0);
				HdfsRead(fichierNom);
				break;
			case "write" : // cas lecture
				envoyerLigneAuServeur("ecriture", 0);
				if (formatFichier.equals("txt")) { // on donne le format txt
					fmt = FileReaderWriter.FMT_TXT;
					HdfsWrite(fmt, fichierNom);
				} else if(formatFichier.equals("kv")) { // on donne le format kv
					fmt =FileReaderWriter.FMT_KV;
					HdfsWrite(fmt, fichierNom);
				} 
				break;
			case "delete" : // cas supprimer
				envoyerLigneAuServeur("supprimer", 0);
				HdfsDelete(fichierNom); 
				break;
			
			default:
				System.out.println("Opération inconnue. \n");
				usage();
		}
	}
}

