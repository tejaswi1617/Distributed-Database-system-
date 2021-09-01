import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.io.*;
import java.util.Scanner;

public class CredentialsValidation {
    public boolean isValidUser() throws IOException {
        String username, password;
        boolean result=false;
        do {
            System.out.println("Enter username and password");

            Scanner scanner = new Scanner(System.in);
            username = scanner.next();
            password = scanner.next();
            BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
            File file = new File(ConfigurationSetup.getDataPathLocal() + "\\userMetaData.txt");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String credentials;
            while ((credentials = br.readLine()) != null) {
                String[] creds = credentials.split("\\^");
                for (int i = 0; i < creds.length - 1; i++) {
                    if (creds[i].equals(username) && encoder.matches(password, creds[++i])) {
                        result=true;
                    }
                }
            }
            if(!result)
            {
                System.out.println("Invalid credentials");
            }
        }while(!result);
        return result;
    }
}
