package com.suizo.video;

import com.google.protobuf.ByteString;
import com.suizo.video.grpc.UploadSummary;
import com.suizo.video.grpc.Video;
import com.suizo.video.grpc.VideoServiceGrpc;
import com.suizo.video.grpc.VideoServiceGrpc.VideoServiceBlockingStub;
import com.suizo.video.grpc.VideoServiceGrpc.VideoServiceStub;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.*;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;



public class ProducerClient {

    private final VideoServiceBlockingStub blockingStub;
    private final VideoServiceStub asyncStub;
    private final ManagedChannel channel;



    public ProducerClient(String host, int port){

        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());

    }



    public ProducerClient(ManagedChannelBuilder<?> channelBuilder) {

        channel = channelBuilder.build();
        blockingStub = VideoServiceGrpc.newBlockingStub(channel);
        asyncStub = VideoServiceGrpc.newStub(channel);

    }



    public void shutdown() throws InterruptedException {


        // Stop accepting new calls and finish ongoing RPCs gracefully

        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);


    }



    public void processFolder(String folderPath) {


        File folder = new File(folderPath);



        if (!folder.isDirectory()) {

            printLog("Invalid directory: " + folderPath);
            return;

        }


        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".mp4"));



        if (files == null || files.length == 0) {

            printLog("No MP4 files found in: " + folderPath);
            return;

        }



        for (File file : files) {

            try {

                uploadVideo(file.getAbsolutePath());

            } catch (Exception e) {

                printLog("Failed uploading " + file.getName() + ": " + e.getMessage());

            }


        }


    }



    public void uploadVideo(String videoPath) throws InterruptedException, NoSuchAlgorithmException {


        System.out.println("Sending " + videoPath);


        String clientVideoId = UUID.randomUUID().toString(); // generates a unique id for video
        final CountDownLatch finishLatch = new CountDownLatch(1);
        File videoFile = new File(videoPath);



        // 1. attach video-id and file-name as grpc metadata to aync stub

        Metadata.Key<String> VIDEO_ID_KEY = Metadata.Key.of("video-id", Metadata.ASCII_STRING_MARSHALLER);
        Metadata.Key<String> FILE_NAME_KEY = Metadata.Key.of("file-name", Metadata.ASCII_STRING_MARSHALLER);

        Metadata metadata = new Metadata();
            metadata.put(VIDEO_ID_KEY, clientVideoId);
            metadata.put(FILE_NAME_KEY, videoFile.getName());



        String hashAlgorithm = "SHA-256";


        VideoServiceGrpc.VideoServiceStub metadataBoundStub = asyncStub.withInterceptors(
                MetadataUtils.newAttachHeadersInterceptor(metadata)
        );



        // waits for server response

        StreamObserver<UploadSummary> responseObserver = new StreamObserver<UploadSummary>() {



            // 7. Called and prints summary

            @Override

            public void onNext(UploadSummary summary) {

                System.out.println("\n--- Server Response ---");
                System.out.println("Status: " + summary.getStatus());
                System.out.println("Checksum Size: " + summary.getFinalFileChecksum().size() + " bytes");

            }



            @Override

            public void onError(Throwable t) {

                System.err.println("\n[ERROR] Stream failed: " + t.getMessage());
                finishLatch.countDown();

            }


            // 7. Called and latch counts down
            @Override

            public void onCompleted() {
                finishLatch.countDown();
            }



        };



        // 2. opens client streaming RPC to server (metadata bound stub)

        StreamObserver<Video> requestObserver = metadataBoundStub.uploadVideo(responseObserver);
        byte[] buffer = new byte[1024*64];
        long uploadedBytes = 0;



        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(videoFile))){


            int bytesRead;
            int sequenceNum = 1;


            while ((bytesRead = inputStream.read(buffer)) != -1){



                // 3. slices up video into 64 kb and hashes data

                ByteString data = ByteString.copyFrom(buffer, 0, bytesRead);
                MessageDigest digest = MessageDigest.getInstance(hashAlgorithm);
                byte[] hashData = digest.digest(data.toByteArray());



                // 4. packages data into a video message

                String fileName = videoFile.getName();
                Video videoByte = Video.newBuilder()
                        .setVideoId(clientVideoId)
                        .setData(data)
                        .setDataSize(data.size())
                        .setDataChecksum(ByteString.copyFrom(hashData)) // hashed data
                        .setHashAlgorithm(hashAlgorithm)
                        .setDataSequence(sequenceNum++) // to make sure the sequence is right
                        .build();



                // 5. sends the video byte streamed to consumer asynchronously
                requestObserver.onNext(videoByte);



                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }



                uploadedBytes += bytesRead;



                // Live terminal output for each chunk

                LocalDateTime currentTime = LocalDateTime.now();

                //printLog("Sent " + uploadedBytes + " bytes");


            }


        } catch (RuntimeException e) {

            // Cancel RPC
            requestObserver.onError(e);
            throw e;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        // 6. Mark the end of requests which calls response onCompleted

        requestObserver.onCompleted();


        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            printLog("[TIMEOUT] Upload timed out for: " + videoFile.getName());
        }


    }



    private void printLog(String text) {
        System.out.printf("[%tT] [%s] %s%n", LocalDateTime.now(), Thread.currentThread().getName(), text);
    }



}
