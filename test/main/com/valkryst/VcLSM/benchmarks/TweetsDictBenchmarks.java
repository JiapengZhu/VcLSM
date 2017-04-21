package main.com.valkryst.VcLSM.benchmarks;
import main.com.valkryst.VcLSM.C;
import main.com.valkryst.VcLSM.Tree;
import main.com.valkryst.VcLSM.node.NodeBuilder;
import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;


public class TweetsDictBenchmarks extends Thread {
    private final static AtomicInteger count = new AtomicInteger(0);
    private final static ArrayList<String> keyArr = new ArrayList<>();
    private final static File file = new File("res/Test Data/Tweets.dict");
    private static DB db;
    private final static int nThread = 2;
    private final static Tree tree = new Tree(1000);

    @BeforeClass
    public static void initializeBenchmark(){

        try (final Scanner sc = new Scanner(file)) {
            db = factory.open(new File("levelDB/"), new Options());

            sc.forEachRemaining(string -> {
                final String[] contentArr = sc.next().split(",");
                final String key = contentArr[0];
                keyArr.add(key);
            });
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void benchmarkLevelDB() {
        final long startTime = System.currentTimeMillis();

        final ExecutorService executor = Executors.newFixedThreadPool(nThread);
        Future<?> future = null;

        try(final Scanner sc = new Scanner(file)) {

            while (sc.hasNext()) {
                final String[] contentArr = sc.next().split(",");
                final String key = contentArr[0];
                final String value = contentArr[1];

                future = executor.submit(() -> {
                    db.put(bytes(key), bytes(value));
                    count.incrementAndGet();
                });
            }

            if (future != null) {
                future.get(C.DELAY, TimeUnit.SECONDS);
            }
        } catch (final InterruptedException | IOException | ExecutionException e) {
            e.printStackTrace();
        } catch (final TimeoutException e) {
            future.cancel(true); //interrupt the job
            e.printStackTrace();
        } finally {
            executor.shutdownNow();

            final long elapsedTime = System.currentTimeMillis() - startTime;
            printReport("LevelDB Put Operation:", elapsedTime);
            count.set(0);
        }
    }

    @Test
    public void benchmarkCLSM() {
        final long startTime = System.currentTimeMillis();

        final ExecutorService executor = Executors.newFixedThreadPool(nThread);
        Future<?> future = null;

        final NodeBuilder builder = new NodeBuilder();

        try (final Scanner sc = new Scanner(file)) {
            while (sc.hasNext()) {
                final String[] contentArr = sc.next().split(",");
                final String key = contentArr[0];
                final String value = contentArr[1];

                future = executor.submit(() -> {
                    tree.put(builder.setKey(key).setValue(value).build());
                    builder.reset();
                    count.incrementAndGet();
                });
            }

            if (future != null) {
                future.get(C.DELAY, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | FileNotFoundException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            future.cancel(true); //interrupt the job after C.DELAY
            e.printStackTrace();
        } finally {
            executor.shutdownNow();

            final long elapsedTime = System.currentTimeMillis() - startTime;
            printReport("VcLSM Put Operation:", elapsedTime);
            count.set(0);
        }
    }

    @Test
    public void benchmarkLevelDBGetOpt() {
        final long startTime = System.currentTimeMillis();

        final ExecutorService executor = Executors.newFixedThreadPool(nThread);
        Future<?> future = null;

        try {
            for (final String key : keyArr){
                future = executor.submit(() -> {
                    asString(db.get(bytes(key)));
                    count.incrementAndGet();
                });
            }

            if (future != null) {
                future.get(C.DELAY, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            final long elapsedTime = System.currentTimeMillis() - startTime;
            printReport("LevelDB Get Operation:", elapsedTime);
            count.set(0);
        }
    }

    @Test
    public void benchmarkCLSMGetOpt(){
        final long startTime = System.currentTimeMillis();

        final ExecutorService executor = Executors.newFixedThreadPool(nThread);
        Future<?> future = null;

        try {
            for (final String key : keyArr){
                future = executor.submit(() -> {
                    tree.get(key);
                    count.incrementAndGet();
                });
            }

            if (future != null) {
                future.get(C.DELAY, TimeUnit.SECONDS);
            }
        } catch (final InterruptedException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            final long elapsedTime = System.currentTimeMillis() - startTime;
            printReport("VcLSM Get Operation:", elapsedTime);
            count.set(0);
        }
    }


    @AfterClass
    public static void stopBenchmark() {
        try {
            keyArr.clear();
            db.close();
            FileUtils.deleteDirectory(new File("data/"));
            FileUtils.deleteDirectory(new File("levelDB/"));
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Prints a report for the most recently run benchmark.
     *
     * @param operationName
     *         The name of the benchmark operation.
     *
     * @param elapsedTimeInMillis
     *         The time it took for the benchmark to complete from start to finish.
     */
    private void printReport(final String operationName, final long elapsedTimeInMillis){
        final double throughput = (double)count.get() / (double) elapsedTimeInMillis;

        final String report = operationName +
                              "\n\tNumber of Threads:\t" + nThread +
                              "\n\tTotal Operations:\t" + count.get() +
                              "\n\tElapsed Time:\t" + elapsedTimeInMillis + "ms" +
                              "\n\tThroughput:\t" + throughput + " Operations/Millisecond\n\n";

        System.out.println(report);
    }
}

