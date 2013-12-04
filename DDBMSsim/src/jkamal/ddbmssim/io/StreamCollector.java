package jkamal.ddbmssim.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamCollector extends Thread {
    InputStream is;
    String type;
    String line = null;

    public StreamCollector(InputStream is, String type) {
        this.is = is;
        this.type = type;
    }

    @Override
    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            
            while ((line = br.readLine()) != null)
                System.out.println(type + "> " + line);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}