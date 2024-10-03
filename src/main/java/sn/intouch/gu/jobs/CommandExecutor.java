package sn.intouch.gu.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CommandExecutor {

    public static void executeCommand(String command) {
        String os = System.getProperty("os.name").toLowerCase();
        ProcessBuilder processBuilder = new ProcessBuilder();

        // Vérifier le système d'exploitation et configurer la commande
        if (os.contains("win")) {
            // Windows
            processBuilder.command("cmd.exe", "/c", command);
        } else {
            // Linux ou MacOS
            //processBuilder.command("bash", "-c", command);
            processBuilder.command("bash", command);
        }

        try {
            // Démarrer le processus
            Process process = processBuilder.start();

            // Lire la sortie standard (sortie de la commande)
            InputStream inputStream = process.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);  // Afficher chaque ligne de la sortie
            }

            // Attendre que le processus se termine et obtenir le code de sortie
            int exitCode = process.waitFor();
            System.out.println("\nExited with error code : " + exitCode);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // Remettre le drapeau 'interrupted'
            e.printStackTrace();
        }
    }

}
