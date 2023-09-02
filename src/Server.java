package com.mycompany.moviescorebackendserver;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.UnknownHostException;

public class Server {

    public static void main(String args[]) {
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(4242);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("Server is up!");
        System.out.println("Listening on: " + serverSocket.getLocalSocketAddress());
        try {
            System.out.println(Inet4Address.getLocalHost().getHostAddress());
        } catch (UnknownHostException ex) {
            System.out.println("ERROR:  in gethostadress");
        }

        while (true) {
            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
                Connection c = new Connection(clientSocket);
                c.start();
                //clientSocket.close();
            } catch (IOException e) {
                System.out.println("ERROR: in server while");
                System.out.println(e);
            }

        }
    }

}
