package config;

public class Project {
	//public static String PATH = "/home/hagimont/hagidoop/";
	//public static String PATH = "C:/Users/UserPC/Documents/GitHub/Projet-Donnees-Reparties/hagidoop/"; // Yanis
	// public static String PATH = "/home/yelalami/Bureau/GitHub/Projet-Donnees-Reparties/hagidoop/"; //  Youssef
	public static String PATH = "/home/tom/Documents/Projet-Donnees-Reparties/hagidoop/"; // Tom
	public static int nbNoeud = 3;
	public static String hosts[] = {"taudard@balrog", "taudard@taupiqueur", "taudard@salameche"};
	public static int ports[] = {8100, 8101, 8102};
	public static int portWorker = 8103;
	public static String hostInit = "localhost";
	public static int portInit = 8080;
}
