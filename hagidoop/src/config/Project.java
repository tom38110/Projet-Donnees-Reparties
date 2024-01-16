package config;

public class Project {
	//public static String PATH = "/home/hagimont/hagidoop/";
	//public static String PATH = "C:/Users/UserPC/Documents/GitHub/Projet-Donnees-Reparties/hagidoop/"; // Yanis
	// public static String PATH = "/home/yelalami/Bureau/GitHub/Projet-Donnees-Reparties/hagidoop/"; //  Youssef
	// public static String PATH = "/home/taudard/Intergiciel/PDR/Projet-Donnees-Reparties/hagidoop/"; // Tom machine n7
	public static String PATH = "/home/tom/Documents/Projet-Donnees-Reparties/hagidoop/"; // Tom
	public static int nbNoeud = 3;
	public static String hosts[] = {"localhost", "localhost", "localhost"};
	public static int ports[] = {9401, 8102, 8103};
	public static int portWorker = 8104;
	public static String hostInit = "localhost";
	public static int portInit = 8080;
}
