package com.mycompany.moviescorebackendserver;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Connection extends Thread implements Serializable {

    private Socket clientSocket = null;
    private  ObjectInputStream objectInput = null;

    public Connection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        System.out.println("New connection, port:" + clientSocket.getPort() + " IP:" + clientSocket.getInetAddress());
    }

    @Override
    public void run() {
        String message = "";
        try {
            objectInput = new ObjectInputStream(clientSocket.getInputStream());
            Object object = objectInput.readObject();
            message = (String) object;
        } catch (IOException ex) {
            System.out.println("ERROR: in connection in bufferedReader");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (message.equalsIgnoreCase("recommend")) {
            giveRecommendation();
        } else if (message.equalsIgnoreCase("getMoviesToRate")) {
            giveMovieInfos();
        } else if (message.equalsIgnoreCase("search")) {
            giveSearchResults();
        } else if (message.equalsIgnoreCase("searchForRecommendation")) {
            giveSpecificRecommendation();
        } else if (message.equalsIgnoreCase("returnAllRecommendationsAbove4")) {
            returnAllRecommendationsAbove4();
        } else {
            System.out.println("not supported action");
        }
        try {
            clientSocket.close();
        } catch (IOException ex) {
            System.out.println("ERROR: while closing the socket");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private ArrayList<String> replaceInArrayList(String stringToReplace, String newString, ArrayList<String> ar){
        String line = "";
        for(int i=0;i<ar.size();i++){
            line = ar.get(i).replaceAll(stringToReplace, newString);
            ar.remove(i);
            ar.add(i, line);
        }            
       return ar;        
    }
    
    
    
    private void returnAllRecommendationsAbove4() {
        ArrayList<String> userRatings = null;
        try {
            System.out.println("reading data from network");
            Object object = objectInput.readObject();
            userRatings = (ArrayList<String>) object;
            System.out.println("done reading data from network");
        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (userRatings == null) {
            System.out.println("userRatings is null returning");
            return;
        }
        System.out.println("Preparing recommendation...");
        ArrayList<String> recom = replaceInArrayList("&amp;", "&", MFSCCD.getAllRecommendationsAbove4(userRatings));
        System.out.println("Recomendation done!");
        ObjectOutputStream objectOutput;
        try {
            objectOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            objectOutput.writeObject(recom);
        } catch (IOException ex) {
            System.out.println("ERROR: in objectOutput");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }     
    }

    private void giveSpecificRecommendation() {
        String movieName = "dnthatinpatiswetsi";
        try {
            objectInput = new ObjectInputStream(clientSocket.getInputStream());
            Object object = objectInput.readObject();
            movieName = (String) object;
        } catch (IOException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Searching for the movie");
        ArrayList<String> movieInfos = replaceInArrayList("&amp;", "&",DBTools.searchForAMovie(movieName));

        ArrayList<String> userRatings = null;
        try {
            System.out.println("reading data from network");
            Object object = objectInput.readObject();
            userRatings = (ArrayList<String>) object;
            System.out.println("done reading data from network");
        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (userRatings == null) {
            System.out.println("userRatings is null returning");
            return;
        }
        System.out.println("Preparing recommendation...");

        ArrayList<String> recom = replaceInArrayList("&amp;", "&",MFSCCD.getSpecificRecommendation(userRatings, movieInfos));

        System.out.println("Recomendation done!");
        ObjectOutputStream objectOutput;
        try {
            objectOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            objectOutput.writeObject(recom);
        } catch (IOException ex) {
            System.out.println("ERROR: in objectOutput");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void giveSearchResults() {
        String movieName = "dnthatinpatiswetsi";
        try {
            objectInput = new ObjectInputStream(clientSocket.getInputStream());
            Object object = objectInput.readObject();
            movieName = (String) object;
        } catch (IOException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Searching for the movie");
        ArrayList<String> movieInfos = replaceInArrayList("&amp;", "&",DBTools.searchForAMovie(movieName));

        System.out.println("Sending Search results to client");
        ObjectOutputStream objectOutput;
        try {
            objectOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            objectOutput.writeObject(movieInfos);
            objectOutput.flush();
        } catch (IOException ex) {
            System.out.println("ERROR: in objectOutput");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void giveMovieInfos() {
        System.out.println("Getting movieInfos");
        ArrayList<String> movieInfos = replaceInArrayList("&amp;", "&",DBTools.getRandomMoviesFromMovieInfo());

        System.out.println("Sending movieInfos to client");
        ObjectOutputStream objectOutput;
        try {
            objectOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            objectOutput.writeObject(movieInfos);
            objectOutput.flush();
        } catch (IOException ex) {
            System.out.println("ERROR: in objectOutput");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void giveRecommendation() {
        ArrayList<String> userRatings = null;
        try {
            System.out.println("reading data from network");
            Object object = objectInput.readObject();
            userRatings = (ArrayList<String>) object;
            System.out.println("done reading data from network");
        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (userRatings == null) {
            System.out.println("userRatings is null returning");
            return;
        }
        System.out.println("Preparing recommendation...");
        ArrayList<String> recom = new ArrayList<String>();
        recom = replaceInArrayList("&amp;", "&",MFSCCD.getRecommendations(userRatings));
        
        System.out.println("Recomendation done!");
        ObjectOutputStream objectOutput;
        try {
            objectOutput = new ObjectOutputStream(clientSocket.getOutputStream());
            objectOutput.writeObject(recom);
        } catch (IOException ex) {
            System.out.println("ERROR: in objectOutput");
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
       
    }
}
