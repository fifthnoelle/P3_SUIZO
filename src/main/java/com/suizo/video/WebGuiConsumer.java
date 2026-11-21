package com.suizo.video;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;



public class WebGuiConsumer {

    private final HttpsServer httpsServer;


    public WebGuiConsumer(int port) throws IOException {


        try {

            char[] password = "password".toCharArray();
            KeyStore ks = KeyStore.getInstance("PKCS12");


            try (FileInputStream fis = new FileInputStream("server_keystore.p12")) {
                ks.load(fis, password);
            }



            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, password);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ks);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);



            httpsServer = HttpsServer.create(new InetSocketAddress(port), 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
            httpsServer.createContext("/", this::handleIndex);
            httpsServer.createContext("/api/videos", this::handleVideoList);
            httpsServer.createContext("/videos/", this::handleVideoStream);
            httpsServer.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());

        } catch (Exception e) {
            throw new IOException("Failed to initialize HTTPS server: " + e.getMessage(), e);
        }


    }



    public void start() {
        httpsServer.start();
        System.out.println("Consumer Web GUI running at: https://localhost:" + httpsServer.getAddress().getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> httpsServer.stop(0)));
    }



    private void handleIndex(HttpExchange exchange) throws IOException {

        String html =
            """
            
            <!DOCTYPE html>
            
            <html lang="en">
            
            <head>
            
            
                <meta charset="UTF-8">
                
                <title>Consumer</title>
                
                
                <style>
                
                
                    body { 
                        font-family: serif; 
                        background: #121212; 
                        color: #fff; 
                        margin: 20px;                
                    }
                    
                    h1 { 
                        margin-bottom: 5px; 
                    }
                    
                    p.sub { 
                        color: #aaa; 
                        margin-top: 0; 
                        margin-bottom: 20px; 
                    }
                    
                    .grid { 
                        display: grid; 
                        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); 
                        gap: 16px;
                    }
                    
                    .card { 
                        background: #1e1e1e; 
                        border-radius: 8px; 
                        overflow: hidden; 
                        cursor: pointer; 
                        border: 1px solid #333; 
                        transition: transform 0.15s; 
                    }
                    
                    .card:hover { 
                        transform: scale(1.02); 
                        border-color: #4da3ff; 
                    }
                    
                    .card video { 
                        width: 100%; 
                        height: 180px; 
                        object-fit: cover; 
                        background: #000; 
                        display: block; 
                    }
                    
                    .card .title { 
                        padding: 10px; 
                        font-size: 13px; 
                        white-space: nowrap; 
                        overflow: hidden; 
                        text-overflow: ellipsis; 
                    }
                    
                    
                    
                    /* Modal */
                    #modal {
                        display: none;
                        position: fixed;
                        inset: 0;
                        background: rgba(0,0,0,0.85);
                        align-items: center;
                        justify-content: center;
                        z-index: 100;
                        }
                
                    #modal video {
                        max-width: 90vw; 
                        max-height: 80vh;
                        border-radius: 4px;
                        background: #000;
                    }
                
                    #closeBtn {
                        position: absolute;
                        top: 20px;
                        right: 30px;
                        padding: 8px 16px;
                        font-size: 16px;
                        font-family: sans-serif;
                        cursor: pointer;
                        background: transparent;
                        color: #aaa;
                        border: 1px solid transparent;
                        border-radius: 4px;
                        transition: all 0.2s ease;
                        z-index: 101;
                    }

                    #closeBtn:hover {
                        background: rgba(255, 255, 255, 0.1);
                        color: #fff;
                        border-color: #fff;
                    }
                    
                    
                </style>
                
                
            </head>
            
       
            
            <body>
            
            
                <h1>Uploaded Videos</h1>
                <p class="sub">Hover over a video for a 10 second preview. Click to watch full video.</p>
                <div class="grid" id="gallery">Loading videos...</div>


                <div id="modal">
                        <div style="position: absolute; top: 20px; left: 30px; z-index: 101; font-family: sans-serif;">
                            <h2 id="modalTitle" style="margin: 0; font-size: 22px; text-shadow: 1px 1px 4px #000;"></h2>
                            <div id="modalId" style="color: #aaa; font-size: 13px; margin-top: 4px;"></div>
                        </div>
                        <button id="closeBtn" onclick="closeModal()"><strong>X</strong></button>
                        <video id="modalPlayer" controls autoplay></video>
                </div>



                <script>
                
                
                let currentFilesJSON = "";
                async function loadVideos() {
                
                    const res = await fetch('/api/videos');
                    const textRes = await res.text();
                
                
                    if (textRes === currentFilesJSON) return;
                
                
                    currentFilesJSON = textRes; // Update our tracker
                    const files = JSON.parse(textRes);
                    const gallery = document.getElementById('gallery');
                
                
                    if (files.length === 0) {
                        gallery.innerHTML = '<p>No videos uploaded yet.</p>';
                        return;
                    }
                
                
                
                    gallery.innerHTML = '';
                    files.forEach(file => {
                    
                        const url = '/videos/' + encodeURIComponent(file.filename);
                        const card = document.createElement('div');
                        card.className = 'card';
                
                
                        const video = document.createElement('video');
                        video.src = url;
                        video.muted = true;
                        video.preload = 'metadata';
                
                
                        
                        video.addEventListener('timeupdate', () => {
                            if (video.currentTime >= 10) {
                                video.currentTime = 0; // Loop first 10s while hovering
                            }
                        });
                
                
                
                        card.addEventListener('mouseenter', () => {
                            video.currentTime = 0;
                            video.play().catch(() => {});
                        });
                
                
                
                        card.addEventListener('mouseleave', () => {
                            video.pause();
                            video.currentTime = 0;
                        });
                
                
                
                        // click to display video
                        card.addEventListener('click', () => openModal(url, file.name, file.id));
                
                
                        const title = document.createElement('div');
                        title.className = 'title';
                        title.innerHTML = `${file.name}`;
                
                
                        card.appendChild(video);
                        card.appendChild(title);
                        gallery.appendChild(card);
                        
                    });
                    
                }
                
                

                    function openModal(url, name, id) {
                    
                    
                        const modal = document.getElementById('modal');
                        const player = document.getElementById('modalPlayer');
                        
                        document.getElementById('modalTitle').textContent = name;
                        document.getElementById('modalId').textContent = "ID: " + id;
                                                    
                        player.src = url;
                        modal.style.display = 'flex';
                        
                        
                    }



                    function closeModal() {
                    
                        const modal = document.getElementById('modal');
                        const player = document.getElementById('modalPlayer');
                        player.pause();
                        player.src = '';
                        modal.style.display = 'none';
                        
                    }



                    loadVideos();
                    
                    
                    
                    // refresh gallery every 5 seconds for incoming uploads
                    
                    setInterval(() => {
                        if (document.getElementById('modal').style.display !== 'flex') {
                            loadVideos();
                        }
                    }, 5000);
                    
                    
                </script>
                
                
            </body>
            
            
            </html>
            
            """;



        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, bytes.length);


        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }


    }



    // 2. Returns JSON array of filenames in uploadedvideos/

    private void handleVideoList(HttpExchange exchange) throws IOException {


        File dir = new File("uploadedvideos");
        String json = "[]";



        if (dir.exists() && dir.isDirectory()) {

            File[] files = dir.listFiles((d, name) ->
                    name.toLowerCase().endsWith(".mp4") && new File(d, name + ".meta").exists()
            );


            if (files != null) {

                Arrays.sort(files, (a, b) -> a.getName().compareTo(b.getName()));


                json = Arrays.stream(files).map(f -> {
                    String filename = f.getName();
                    String videoId = "Unknown";
                    String videoName = filename;


                    File metaFile = new File(dir, filename + ".meta");


                    if (metaFile.exists()) {

                        try (FileInputStream fis = new FileInputStream(metaFile)) {
                            Properties props = new Properties();
                            props.load(fis);
                            videoId = props.getProperty("id", videoId);
                            videoName = props.getProperty("name", videoName);

                        } catch (IOException ignored) {}

                    }

                    return String.format("{\"filename\":\"%s\", \"id\":\"%s\", \"name\":\"%s\"}",
                            filename.replace("\"", "\\\""),
                            videoId.replace("\"", "\\\""),
                            videoName.replace("\"", "\\\""));



                }).collect(Collectors.joining(",", "[", "]"));

            }

        }


        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);


        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }


    }



    // Streams MP4 bytes with HTTP Range support for HTML5 <video> seeking

    private void handleVideoStream(HttpExchange exchange) throws IOException {

        String fileName = exchange.getRequestURI().getPath().substring("/videos/".length());
        File file = new File("uploadedvideos", fileName);


        if (!file.exists() || !file.getCanonicalPath().startsWith(new File("uploadedvideos").getCanonicalPath())) {

            exchange.sendResponseHeaders(404, -1);
            return;

        }


        long fileLength = file.length();
        String rangeHeader = exchange.getRequestHeaders().getFirst("Range");


        if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {

            String[] ranges = rangeHeader.substring(6).split("-");
            long start = Long.parseLong(ranges[0]);
            long end = (ranges.length > 1 && !ranges[1].isEmpty()) ? Long.parseLong(ranges[1]) : fileLength - 1;
            long contentLength = end - start + 1;



            exchange.getResponseHeaders().set("Content-Type", "video/mp4");
            exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
            exchange.getResponseHeaders().set("Cache-Control", "no-store"); // FIX: Prevent browser caching broken streams
            exchange.getResponseHeaders().set("Content-Range", "bytes " + start + "-" + end + "/" + fileLength);
            exchange.sendResponseHeaders(206, contentLength);


            try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                 OutputStream os = exchange.getResponseBody()) {


                raf.seek(start);
                byte[] buffer = new byte[8192];
                long remaining = contentLength;


                while (remaining > 0) {
                    int read = raf.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read == -1) break;
                    os.write(buffer, 0, read);
                    remaining -= read;
                }

            } catch (IOException ignored) {
            }

        } else {

            exchange.getResponseHeaders().set("Content-Type", "video/mp4");
            exchange.sendResponseHeaders(200, fileLength);


            try (OutputStream os = exchange.getResponseBody()) {
                Files.copy(file.toPath(), os);
            }

        }


    }


}
