package main.com.valkryst.VcLSM.benchmarks;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by jiapengzhu on 2017-04-19.
 */
public class DataProcessing {
    public void readDataFromDataset(String fileName, String delimeter, int keyIndex){

        try {
            File file = new File(fileName);
            Scanner sc = new Scanner(file);
            String content = null;
            while(sc.hasNext()){
                content = sc.next();
                String[] contentArr = content.split(delimeter);


            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }
}
