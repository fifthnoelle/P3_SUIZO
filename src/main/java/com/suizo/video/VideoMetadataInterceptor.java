package com.suizo.video;

import io.grpc.*;



public class VideoMetadataInterceptor implements ServerInterceptor {

    public static final Metadata.Key<String> VIDEO_ID_META_KEY = Metadata.Key.of("video-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> VIDEO_ID_CTX_KEY = Context.key("videoId");


    // Keys for the video name
    public static final Metadata.Key<String> VIDEO_NAME_META_KEY = Metadata.Key.of("file-name", Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<String> VIDEO_NAME_CTX_KEY = Context.key("videoName");



    @Override

    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        // Extract the video ID and Name from metadata

        String videoId = headers.get(VIDEO_ID_META_KEY);
        String videoName = headers.get(VIDEO_NAME_META_KEY);


        if (videoName == null) {
            videoName = videoId; // fallback just in case
        }


        // Make both available to the service methods

        Context ctx = Context.current()
                .withValue(VIDEO_ID_CTX_KEY, videoId)
                .withValue(VIDEO_NAME_CTX_KEY, videoName);

        return Contexts.interceptCall(ctx, call, headers, next);

    }

}