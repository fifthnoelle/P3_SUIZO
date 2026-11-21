package com.suizo.video;

import java.io.IOException;
import java.io.FileInputStream;

public class VideoBuffer {
    private FileInputStream videoInput;

    public VideoBuffer(FileInputStream videoInput){
        this.videoInput = videoInput;
    }

}
