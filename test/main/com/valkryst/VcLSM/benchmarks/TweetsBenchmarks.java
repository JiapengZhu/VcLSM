package main.com.valkryst.VcLSM.benchmarks;

import main.com.valkryst.VcLSM.C;
import main.com.valkryst.VcLSM.Tree;
import main.com.valkryst.VcLSM.node.Node;
import main.com.valkryst.VcLSM.node.NodeBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.*;
import org.iq80.leveldb.*;
import org.junit.*;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jiapengzhu on 2017-04-19.
 */
public class TweetsBenchmarks extends Thread{
    private long startTime, endTime, gcRunningTime;
    private static ReentrantLock lock;
    private static int count;
    private static ArrayList<String> keyArr;
    private static org.apache.logging.log4j.Logger logger;
    private static String fileName;
    private static File file;
    private static int nThread;
    private static DB db;
    private static Options options;
    private static Tree tree;

    @BeforeClass
    public static void initializeBenchmark(){
        keyArr = new ArrayList<String>();
        count = 0;
        logger = LogManager.getLogger();
        lock = new ReentrantLock();
        Scanner sc = null;
        fileName = "src/Tweets2mi"; // dataset name
        nThread = 2; // number of threads
        options = new Options();
        options.createIfMissing(true);
        String content = null;
        try{
            db = factory.open(new File("levelDB/"), options);
            tree = new Tree(1000);
            file = new File(fileName);
            sc = new Scanner(file);
            while(sc.hasNext()){
                content = sc.nextLine();
                String[] contentArr = content.split("\t");
                String key = contentArr[0];
                keyArr.add(key);

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void benchmarkLevelDB(){
        ExecutorService executor = null;
        Future<?> future = null;
        Scanner sc = null;
        String content = null;
        String optName = "LevelDB Put Operation: ";
        try{
            executor = Executors.newFixedThreadPool(nThread); // Set up number of threads
            sc = new Scanner(file);
            while(sc.hasNext()){
                content = sc.nextLine();
                String[] contentArr = content.split("\t");
                String key = contentArr[0];
                String value = contentArr[1];
                future = executor.submit(() -> {
                    db.put(bytes(key), bytes(value));
                    increment();
                });
            }
            future.get(C.DELAY, TimeUnit.SECONDS);
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ExecutionException e) {
            e.printStackTrace();
        }catch (TimeoutException e) {
            future.cancel(true); //interrupt the job
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (!executor.isTerminated()) {
                System.err.println("Cancel non-finished tasks");
            }
            executor.shutdownNow();
            sc.close();
            // System.out.println("Shutdown finished");
            logger.info(printReport(optName));
            System.out.println(printReport(optName));
            benchmarkLevelDBGetOpt();
        }
    }

    @Test
    public void benchmarkCLSM(){
        ExecutorService executor = null;
        Future<?> future = null;
        Scanner sc = null;
        String content = null;
        String optName = "cLSM Put Operation: ";
        try{
            sc = new Scanner(file);
            executor = Executors.newFixedThreadPool(nThread);

            while(sc.hasNext()){
                content = sc.nextLine();
                String[] contentArr = content.split("\t");
                String key = contentArr[0];
                String value = contentArr[1];
                future = executor.submit(() -> {
                    final Node node = new NodeBuilder().setKey(key).setValue(value).build();
                    tree.put(node);
                    increment();
                });
            }
            future.get(C.DELAY, TimeUnit.SECONDS);
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ExecutionException e) {
            e.printStackTrace();
        }catch (TimeoutException e) {
            future.cancel(true); //interrupt the job after C.DELAY
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (!executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            executor.shutdownNow();
            sc.close();
            System.out.println("shutdown finished");
            logger.info(printReport(optName));
            System.out.println(printReport(optName));
            benchmarkCLSMGetOpt();
        }
    }


    private void benchmarkLevelDBGetOpt(){
        clearCounter();
        ExecutorService executor = null;
        Future<?> future = null;
        executor = Executors.newFixedThreadPool(nThread);
        String optName = "LevelDB Get Operation: ";
        try {
            for(String key : keyArr){
                future = executor.submit(() -> {
                    String value = asString(db.get(bytes(key)));
                    increment();
                });
            }
            future.get(C.DELAY, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {
            System.out.println("\n" + printReport(optName));
            clearCounter();
        }
    }

    private void benchmarkCLSMGetOpt(){
        clearCounter();
        ExecutorService executor = null;
        Future<?> future = null;
        executor = Executors.newFixedThreadPool(nThread);
        String optName = "cLSM Get Operation: ";
        try {
            for(String key : keyArr){
                future = executor.submit(() -> {
                    Optional<Node> retrievedNode = tree.get(key);
                    increment();
                });
            }
            future.get(C.DELAY, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {
            System.out.println("\n" + printReport(optName));
            clearCounter();
        }
    }


    @AfterClass
    public static void stopBenchmark(){
        try {
            keyArr.clear();
            FileUtils.deleteDirectory(new File("data/"));
            FileUtils.deleteDirectory(new File("levelDB/"));
            db.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int increment(){
        lock.lock();
        try {
            count++;
        } finally {
            lock.unlock();
        }
        return count;
    }

    private void clearCounter(){
        count = 0;
    }

    private String printReport(String optName){
        double throughput = (double)count / (double)C.DELAY;
        String report = optName + "\nNumber of threads: " + nThread +
                "\nTotal operation counts: " + count +
                "\nTime eclipse: " + C.DELAY + " sec" +
                "\nThroughput: " + throughput + " opts/sec";
        return report;
    }

}
