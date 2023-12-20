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
	//public static String hosts[] = {"localhost", "localhost", "localhost"};
	//public static int ports[] = {8081, 8082, 8083};
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	
	
	/* Probablement fonctionnel (en attente de test ) */
	/*public static void HdfsDelete(String fname) {
		try {
		// File fichier = new File(~/Documents/Projet-Donnees-Reparties
		//						+ fname); // modifier le path avec celui de Hagimont
		File fichier = new File(Project.PATH + "data/" + fname); // modifier le path avec celui de Hagimont
		if (fichier.delete()) {
			System.out.println("le fichier : " + fname + " a été effacé.");
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	
	
	
	// fmt format du fichier (FMT_TXT ou FMT_KV)
	// lis chaque ligne du fichier sans le découper et stock les fragments dans les différents noeuds

	//Passer par les modulo pour traiter les lignes
	
	public static void HdfsWrite(int fmt, String fname) {

		int nbServ = Project.nbNoeud; // nombre de serveur = nombre de noeuds 
		
		ArrayList<String> lignes = new ArrayList<>(); // liste des lignes stockées 

		File fichier = new File(Project.PATH + "data/" +  fname); // modifier le path avec celui de Hagimont

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

		int nbLigneFrag = lignes.size()/ nbServ; // Nombre de ligne par fragment.
		String fragName;
		
		/*FileReaderWriter txtFRW;
		FileReaderWriter txtFRWFrag;
		FileReaderWriter kvFRW;
		FileReaderWriter kvFRWFrag;*/
		if (fmt == 0) { // FMT_TXT	
			for (int i= 0; i<nbServ; i++) {
				fragName = "fragment-" + i + fname;
				File fragment = new File( Project.PATH + "data/" + fragName);
				
				try {
					fragment.createNewFile();
					FileWriter fragmentWriter = new FileWriter(fragment);

					// indices de début et de fin des fragments
					int debut =i*nbLigneFrag;
					int fin =(i+1)*nbLigneFrag;

					for (int j = debut; j <= fin; j++) {
						/*txtFRWFrag = new TxtFileReaderWriter(fragName,j);
						txtFRW = new TxtFileReaderWriter(fname,j);
						KV kv = txtFRW.read(); // Lecture Fichier
						txtFRWFrag.write(kv); // Ecriture Fragment*/

						// ATTENTION Traiter les textes non divisible par nbNoeud et gérer les doubles noeuds
						String ligne = lignes.get(j);
        				fragmentWriter.write(ligne); // Ecriture Fragment
        				fragmentWriter.write(System.lineSeparator());

					}

					fragmentWriter.close();
					
					envoyerFragmentAuServeur(fragName, i);
				}catch (IOException e) {
					e.printStackTrace();
				}
			}

		} else if (fmt == 1) {// FMT_KV
			
			for (int i= 0; i<nbServ; i++) {
				fragName = "fragment-" + i + fname;
				File fragment = new File( Project.PATH + "data/" + fragName);
				//String fNameFin = Project.PATH + "data/" + fname;
				try{
					fragment.createNewFile();
					FileWriter fragmentWriter = new FileWriter(fragment);
					// indices de début et de fin des fragments
					int debut =i*nbLigneFrag;
					int fin =(i+1)*nbLigneFrag;

					for (int j = debut; j <= fin; j++) {
						
							/* kvFRWFrag = new KVFileReaderWriter(fragName,j);
							kvFRW = new KVFileReaderWriter(fNameFin,j);
							
							KV kv = kvFRW.read(); // Lecture Fichier
							*/
							//fragmentWriter.write(kv.toString()); // Ecriture Fragment
							String ligne = "null <-> " + lignes.get(j);
							fragmentWriter.write(ligne);
							fragmentWriter.write(System.lineSeparator());
							//System.out.println(kv.k);		
					} 
					fragmentWriter.close();
					envoyerFragmentAuServeur(fragName, i);
				} catch (IOException e){
					e.printStackTrace();
				}
			}
		}
		
	}
	



	private static void envoyerFragmentAuServeur(String fragName, int indiceServeur) {
		Integer port = serveurPorts.get(indiceServeur);
   		if (port != null) {
			try (Socket socket = new Socket(serveurAdresses.get(indiceServeur), serveurPorts.get(indiceServeur));
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
					oos.writeObject("ecriture");
					oos.writeObject(fragName);

					try (BufferedReader bufLectFrag = new BufferedReader(new FileReader(Project.PATH + "data/" +  fragName))) {
						String ligneFrag;
						while ((ligneFrag = bufLectFrag.readLine()) != null ){
							System.out.println("Envoi de la ligne : " + ligneFrag);
							oos.writeObject(ligneFrag);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.out.println("Fin envoie fragment");
					oos.writeObject(null);
					oos.close();
					//Envoyer un message comme quoi le fragment a été correctement envoyé
					System.out.println("le fragment a été correctement envoyé!");

					
					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					String reponse = (String) ois.readObject();

					System.out.println(reponse);

				} catch (IOException | ClassNotFoundException e ){
					e.printStackTrace();
				}
		} else {
			System.out.println("Port non trouvé");
		}
	}




	public static void HdfsRead(String fname) {
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
		}
	}




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
			/*case "delete" : // cas supprimer
				HdfsDelete(fichierNom); 
				break;
			*/
			default:
				System.out.println("Opération inconnue. \n");
				usage();
		}
	}
}

