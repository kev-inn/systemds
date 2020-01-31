package org.tugraz.sysds.pythonapi;

import py4j.GatewayServer;

public class pythonDMLScript {

    public static void main(String[] args){
        GatewayServer gatewayServer = new GatewayServer(new pythonDMLScript());
        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }

    public String[] createString(){
        return new String[0];
    }


}
