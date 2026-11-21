package com.suizo.video;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import com.suizo.video.grpc.Video;
import com.suizo.video.grpc.UploadSummary;
import com.suizo.video.grpc.VideoServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.*;



public class ConsumerServer {

    private final int port;
    private final Server server;



    public ConsumerServer (int port, int maxConcurrentUploads, int consumerThreads, double bytesPerSecondLimit){

        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(ServerInterceptors.intercept(
                        new VideoService(maxConcurrentUploads, consumerThreads, bytesPerSecondLimit),
                        new VideoMetadataInterceptor()
                ))
                .build(); // creates a new video service thread

    }



    public void start() throws IOException {

        server.start();
        System.out.println("Consumer gRPC Server Started on port " + port);
        new WebGuiConsumer(8080).start();



        //create shutdown thread

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.err.println("*** Shutting down gRPC server");
            if (server != null) {
                server.shutdown();
            }

        }));


    }



    public void blockUntilShutdown() throws InterruptedException {

        if (server != null) {
            server.awaitTermination();
        }

    }



    private static class VideoService extends VideoServiceGrpc.VideoServiceImplBase {

        // pessimistic lock

        private final Semaphore UPLOAD_SEMAPHORE;
        private static final ByteString END_OF_STREAM= ByteString.EMPTY;


        // track actively uploading video IDs to block other streams

        private static final Set<String> ACTIVE_UPLOADS = ConcurrentHashMap.newKeySet();


        // collection of finished hashed videos (for duplicate check)

        private static final Set<String> COMPLETED_FILE_HASHES = ConcurrentHashMap.newKeySet();


        // thread pool

        private final ExecutorService ioThreadPool;
        private final double ioRateLimitBytesPerSec;



        // video service constructor

        public VideoService(int maxConcurrentUploads, int consumerThreads, double ioRateLimitBytesPerSec){

            this.UPLOAD_SEMAPHORE = new Semaphore(maxConcurrentUploads);
            this.ioThreadPool = Executors.newFixedThreadPool(consumerThreads);
            this.ioRateLimitBytesPerSec = ioRateLimitBytesPerSec;


            // recovers existing video hashes

            initializeExistingFileHashes();

        }



        private void initializeExistingFileHashes() {

            File dir = new File("uploadedvideos");


            if (dir.exists() && dir.isDirectory()) { // file exists
                File[] files = dir.listFiles((d, name) -> name.endsWith(".mp4"));  //is mp4


                if (files != null) {  // not empty


                    for (File file : files) {   //for all files


                        // gets hashes

                        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file))) {

                            MessageDigest digest = MessageDigest.getInstance("SHA-256");
                            byte[] buffer = new byte[8192];
                            int read;


                            while ((read = fis.read(buffer)) != -1) {
                                digest.update(buffer, 0, read);
                            }


                            // adds to collection of hashes

                            COMPLETED_FILE_HASHES.add(bytesToHex(digest.digest()));


                            System.out.println("[Init] Indexed existing file: " + file.getName());


                        } catch (Exception e) {
                            System.err.println("[Init] Failed to hash existing file " + file.getName() + ": " + e.getMessage());
                        }


                    }


                }


            }


        }



        // helper bytes to hex

        private static String bytesToHex(byte[] bytes) {

            StringBuilder sb = new StringBuilder();

            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }

            return sb.toString();

        }



        @Override

        public StreamObserver<Video> uploadVideo(final StreamObserver<UploadSummary> responseObserver) {

            final ServerCallStreamObserver<UploadSummary> serverResponseObserver =
                    (ServerCallStreamObserver<UploadSummary>) responseObserver;


            return new StreamObserver<Video>() {

                private boolean permitAcquired = false;
                private boolean idRegistered = false;
                private String fileName = "";
                private String status = "FAILED", hashAlgorithm, clientVideoId, textStatus;
                private int dataCount = 0;

                private final BlockingQueue<ByteString> chunkQueue = new ArrayBlockingQueue<>(100); // queue of each upcoming chunk
                private Future<String> ioTaskFuture;    // to be processed



                // Handle disconnection mid-stream

                {
                    serverResponseObserver.setOnCancelHandler(() -> {
                        printText("Client disconnected stream abruptly.");
                        cleanupAndCancel();     // delete partial file
                    });
                }



                @Override

                public void onNext(Video data) {


                    // 1. if video is new (first onNext call)

                    if (dataCount == 0 && !permitAcquired) {
                        clientVideoId = data.getVideoId();



                        if (!UPLOAD_SEMAPHORE.tryAcquire()) {

                            String textStatus = "REJECTED Server busy. Too many concurrent uploads.";

                            handleError(Status.RESOURCE_EXHAUSTED, textStatus);
                            return; // Abort
                        }



                        // 2. tracks if permit is acquired from semaphore

                        permitAcquired = true;



                        // if there is an active upload

                        if (!ACTIVE_UPLOADS.add(clientVideoId)) {

                            String err = "REJECTED Video ID " + clientVideoId + " is already currently uploading.";

                            handleError(Status.ALREADY_EXISTS, err);
                            return;
                        }



                        // 3. tracks registered current video to active task

                        idRegistered = true;
                        hashAlgorithm = data.getHashAlgorithm();    // Hash algorithm of data stream



                        // 4. accepts and assigns chunk to thread (to be processed by FileConsumerTask) -> 5.
                        ioTaskFuture = ioThreadPool.submit(new FileConsumerTask(
                                clientVideoId, chunkQueue, hashAlgorithm, ioRateLimitBytesPerSec
                        ));


                    }



                    // 6. tracks the total number of chunks sent (to be double checked later)

                    dataCount++;


                    int dataSize = data.getDataSize();
                    ByteString dataByte = data.getData();               // Data stream from client
                    ByteString dataCheckSum = data.getDataChecksum();   // Hash of data stream
                    long dataSequence = data.getDataSequence();         // To cross check with data count

                    printText("Received: " + dataSize + " Sequence: " + dataSequence + " File: " + fileName + " VideoID: " + clientVideoId);



                    // 7. Integrity Checks

                    String metaVideoId = VideoMetadataInterceptor.VIDEO_ID_CTX_KEY.get();


                    // 7.1. if videoId from metadata doesnt match sent videoId
                    if (metaVideoId != null && !metaVideoId.equals(clientVideoId)) {

                        textStatus = ("ID Mismatch! Meta: " + metaVideoId + ", Stream: " + clientVideoId);
                        status = "FAILED_ID_MISMATCH";

                        handleError(Status.INVALID_ARGUMENT, textStatus);
                        return;
                    }



                    // 7.2. if data sequence from client doesnt match expected order (chunks sent at wrong sequence)
                    if (dataSequence != dataCount) {

                        textStatus = ("Sequence Mismatch! Expected: " + dataCount + ", Got: " + dataSequence);
                        status = "FAILED_SEQUENCE_MISMATCH";

                        handleError(Status.INVALID_ARGUMENT, textStatus);
                        return;
                    }



                    try {

                        MessageDigest digest = MessageDigest.getInstance(hashAlgorithm);
                        byte[] computedHashBytes = digest.digest(dataByte.toByteArray());
                        ByteString computedHash = ByteString.copyFrom(computedHashBytes);


                        // 7.3. checks if file is corrupted with hash compare
                        if (!computedHash.equals(dataCheckSum)) {

                            textStatus = ("Chunk corruption detected at sequence: " + dataSequence);
                            status = "FAILED_HASH_MISMATCH";

                            handleError(Status.INVALID_ARGUMENT, textStatus);
                            return;
                        }

                    } catch (NoSuchAlgorithmException e) {

                        textStatus = ("Invalid hash algorithm: " + hashAlgorithm);
                        status = "FAILED";

                        handleError(Status.INVALID_ARGUMENT, textStatus);
                        return;
                    }


                    status = "SUCCESS";
                    printText("Received & Validated: " + dataSize + " bytes, Seq: " + dataSequence);



                    try {


                        // 8. .put() sends good data for processing

                        chunkQueue.put(dataByte);

                    } catch (InterruptedException e) {
                        handleError(Status.INTERNAL, "Upload interrupted.");
                        Thread.currentThread().interrupt();
                        return;
                    }


                }



                @Override

                public void onError(Throwable t) {
                    printText("Stream aborted");
                    cleanupAndCancel();
                }



                @Override

                public void onCompleted() {


                    // 9. Eventual File Validation

                    try {


                        // 9.1 Signal Consumer thread that streaming is done

                        chunkQueue.put(END_OF_STREAM);



                        // 9.2 wait for Consumer Thread to finish disk write & final hash

                        String finalFileHashHex = ioTaskFuture.get();
                        File uploadedFile = new File("uploadedvideos", clientVideoId + ".mp4");


                        // 9.4 checks if final hash is already in file hashes collection
                        if (COMPLETED_FILE_HASHES.contains(finalFileHashHex)) {

                            status = "FAILED_DUPLICATE_CONTENT";

                            if (uploadedFile.exists()) {
                                uploadedFile.delete(); // Remove duplicate file from disk
                            }

                            textStatus = "Upload rejected: Identical file content already exists on server.";
                            printText(textStatus);


                            // creates fail summary to be sent back to client

                            UploadSummary summary = UploadSummary.newBuilder()
                                    .setStatus(status)
                                    .setFinalFileChecksum(ByteString.copyFromUtf8(finalFileHashHex))
                                    .build();


                            // send back to client

                            responseObserver.onNext(summary);
                            responseObserver.onCompleted();
                            return;
                        }



                        // 10. if success

                        COMPLETED_FILE_HASHES.add(finalFileHashHex); // adds video hash to collection


                        // 10.1 create metadata file

                        String metaVideoName = VideoMetadataInterceptor.VIDEO_NAME_CTX_KEY.get();
                        File metaFile = new File("uploadedvideos", clientVideoId + ".mp4.meta");


                        try (PrintWriter out = new PrintWriter(new FileWriter(metaFile))) {
                            out.println("id=" + clientVideoId);
                            out.println("name=" + (metaVideoName != null ? metaVideoName : "Unknown Title"));
                        } catch (IOException e) {
                            System.err.println("Failed to write metadata file for: " + clientVideoId);
                        }


                        // creates success summary to be sent back to client

                        UploadSummary summary = UploadSummary.newBuilder()
                                .setStatus(status)
                                .setFinalFileChecksum(ByteString.copyFromUtf8(finalFileHashHex))
                                .build();



                        //calls client onNext and onComplete

                        responseObserver.onNext(summary);
                        responseObserver.onCompleted();
                        printText("Upload complete for ID: " + clientVideoId + " | File Checksum: " + finalFileHashHex);



                    } catch (Exception e) {
                        handleError(Status.INTERNAL, "Error completing I/O processing: " + e.getMessage());
                    } finally {
                        releaseResources();
                    }


                }



                private void cleanupAndCancel() {

                    if (ioTaskFuture != null) {
                        ioTaskFuture.cancel(true); // Interrupts the active worker thread
                    }


                    if (clientVideoId != null) {
                        File partialFile = new File("uploadedvideos", clientVideoId + ".mp4");

                        if (partialFile.exists()) {
                            partialFile.delete();
                            printText("Cleaned up partial/corrupt file: " + partialFile.getName());
                        }

                    }

                    releaseResources();

                }



                private void releaseResources() {


                    // if not null and is registered to active uploads

                    if (idRegistered && clientVideoId != null) {

                        // remove useless data

                        ACTIVE_UPLOADS.remove(clientVideoId);
                        idRegistered = false;

                    }


                    // if permit is acquired

                    if (permitAcquired) {
                        UPLOAD_SEMAPHORE.release();
                        permitAcquired = false;
                    }


                }



                private void printText(String text){
                    System.out.printf("[%tT] %s%n", LocalDateTime.now(), text);
                }



                private void handleError(Status error, String text){
                    printText(text);
                    cleanupAndCancel();


                    responseObserver.onError(error
                            .withDescription(text)
                            .asRuntimeException());

                }


            };


        }


    }



    // 5. class that processes chunk

    private static class FileConsumerTask implements Callable<String> {

        private final String videoId;
        private final BlockingQueue<ByteString> queue;
        private final String hashAlgorithm;
        private final RateLimiter leakyBucket;



        public FileConsumerTask(String videoId, BlockingQueue<ByteString> queue, String hashAlgorithm, double bytesPerSecond) {

            this.videoId = videoId;
            this.queue = queue;
            this.hashAlgorithm = hashAlgorithm;



            // Smooths out write bursts to fixed bytes/second rate limit

            this.leakyBucket = RateLimiter.create(bytesPerSecond);

        }



        @Override

        public String call() throws Exception {

            // 5.1. looks for uploadedvideos directory

            File dir = new File("uploadedvideos");

            if (!dir.exists()) { // make directory if it doesn't exist
                dir.mkdirs();
            }



            // 5.2. creates File and assigns video name

            File outputFile = new File(dir, videoId + ".mp4");


            MessageDigest fullFileDigest = MessageDigest.getInstance(
                    (hashAlgorithm != null && !hashAlgorithm.isEmpty()) ? hashAlgorithm : "SHA-256"
            );



            // 5.3. creates file output stream for the file and makes buffered output stream (pointing to outputfile loc)
            // stream stays open until closed

            try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile))) {

                while (true) {

                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Consumer task interrupted.");
                    }


                    // 8.1. gets oldest chunk from the queue and waits

                    ByteString chunk = queue.take();



                    // onComplete gives sign to stop

                    if (chunk == ByteString.EMPTY) {
                        break;
                    }



                    // 8.2. turns chunk into bytes

                    byte[] bytes = chunk.toByteArray();



                    // 8.3. acquires permits based on incoming byte payload size (throttling)

                    leakyBucket.acquire(bytes.length);



                    // 8.4. write to file and other succeeding chunks and update running digest

                    outputStream.write(bytes);
                    fullFileDigest.update(bytes);


                }


                outputStream.flush();


            }



            // 9.3 returns hashed video

            return VideoService.bytesToHex(fullFileDigest.digest());


        }


    }


}
