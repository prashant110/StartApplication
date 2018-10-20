/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package startapplication;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

/**
 *
 * @author Prashant
 */

class Addr
{
    String host;
    int port;

    public Addr(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
}
public class StartApplication {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        
        HashMap<Integer,Integer> map = new HashMap<>();
        ArrayList<Addr> list = new ArrayList<>();
        Scanner sc = new Scanner(System.in);
        String fileServerHost = " ", nameServerHost = " ",logServerHost = "";
        int fileServerPort = 0 ,nameServerPort = 0 ,logServerPort=0;
        HashSet<Integer> set = new HashSet<>();
        set.add(0);
        set.add(4);
        set.add(8);
        set.add(3);
        set.add(7);
        set.add(11);
        set.add(9);
        /*set.add(14);
        set.add(19);
        set.add(24);*/
        while (true) {            
            String[] cmd = sc.nextLine().split(" ");
            if(cmd.length == 0)
                continue;
            /*
             * possible commands are:
             * add hostid port on which deploy application is running
             * init logserverURL #N #detectionset where N is number of nodes present in system and detectionSet is 
                number of consecutive node get crashed
             * recovery nodeid
             * crash nodeid
             * crashall to crash all nodes present in system
             * fileserver ip port
             * nameserver ip port
             */
            switch(cmd[0])
            {
                case "add":
                    list.add(new Addr(cmd[1], Integer.parseInt(cmd[2])));
                    break;
                case "fileserver":
                {
                    fileServerHost = cmd[1];
                    fileServerPort = Integer.parseInt(cmd[2]);
                    break;
                }
                case "logserver":
                {
                    logServerHost = cmd[1];
                    logServerPort = Integer.parseInt(cmd[2]);
                    break;
                }
                case "nameserver":
                {
                    nameServerHost = cmd[1];
                    nameServerPort = Integer.parseInt(cmd[2]);
                    break;
                }
                case "init":
                {
                    
                    //String logserverURL = cmd[1];
                    int nodes = Integer.parseInt(cmd[1]);
                    int process = nodes;
                    int k = Integer.parseInt(cmd[2]); //detectionset or CS_request/second
                    int request = Integer.parseInt(cmd[3]);
                    long propogationDelay = cmd.length > 4 ? Long.parseLong(cmd[4]) : 0;
                    int n = nodes/list.size(); // divide number of process equally among all nodes in cluster
                    int id = 0;
                    
                    for(int j = 0; j< list.size(); j++)
                    {
                        System.out.println(list.get(j).host+" "+ list.get(j).port);
                        
                        for(int i = 0; i< n ;i++)
                        {
                            Socket s = new Socket(list.get(j).host, list.get(j).port);
                            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                            map.put(id, j);
                            
                            //System.out.println(id + " :: " +(set.contains(id)));
                            dos.writeUTF("run"+" "+"node_"+" "+id + " "+process+ " "+"D:\\Keys" +" "+k+ " "+
                                        "false"+" "+"true"+" "+fileServerHost+" "+fileServerPort+" "+
                                        logServerHost + " " + logServerPort+ " "+nameServerHost + " "+nameServerPort+" "+ set.contains(id) +" "+request + " " +propogationDelay +" "+list.get(j).host); // appport can also be given at the end
                            dos.flush();
                            nodes--;
                            s.close();
                            id++;
                        } 
                        
                    }
                    /* add all remaining processes in last node of cluster
                     */
                    while(nodes != 0)
                    {
                        Socket s = new Socket(list.get(list.size()-1).host, list.get(list.size()-1).port);  
                        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                        map.put(id, list.size()-1);
                        dos.writeUTF("run"+" "+"node_"+" "+id + " "+process+ " "+"D:\\Keys" +" "+k+ " "+
                                "false"+" "+"true"+" "+fileServerHost+" "+fileServerPort+" "+
                                logServerHost + " " + logServerPort+ " "+nameServerHost + " "+nameServerPort+" "+ set.contains(id)+" "+request + " " +propogationDelay +" "+list.get(list.size()-1).host); // appport can also be given at the end
                        dos.flush();
                        nodes--;
                        s.close();
                    }
                    
                    break;
                }
                case "recovery":
                {
                    /*send command for recovery to deploy application*/
                    int process = Integer.parseInt(cmd[1]);
                    int node = map.get(process);
                    Socket s = new Socket(list.get(node).host, list.get(node).port);
                    DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                    dos.writeUTF("recovery"+" "+process);
                    dos.flush();
                    s.close();
                     break;
                }
                case "crash":
                {
                    /*send command for crash to deploy application*/
                    int process = Integer.parseInt(cmd[1]);
                    int node = map.get(process);
                    Socket s = new Socket(list.get(node).host, list.get(node).port);
                    DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                    dos.writeUTF("crash"+" "+process);
                    dos.flush();
                    s.close();
                    break;
                }
                case "crashall":
                {
                    /*send command to crash all nodes, to deploy application*/
                    for(Integer p: map.keySet())
                    {
                        int n = map.get(p);
                        Socket s = new Socket(list.get(n).host, list.get(n).port);
                        DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                        dos.writeUTF("crash"+" "+p);
                        dos.flush();
                        s.close();
                    }
                    break;
                }
                default:
                    System.out.println("Invalid command");
                    break;
            }
            
        }
    }
    
}
