import java.util.Scanner;
/**
 * Main.java
 *
 * Author: Ruidi Chang
 * andrew id: ruidic
 */
public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        // Interact with user
        System.out.println("Please enter a word: ");
        Scanner scan = new Scanner(System.in);
        String searchWord = scan.nextLine().trim();
        TempestAnalytics t = new TempestAnalytics();
        t.tempest(args[0], searchWord);
    }
}