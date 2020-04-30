package com.james._demo;

public class GracefullyShutdown {
    public static void main(String[] args) throws InterruptedException {
        for (int i = 1; i < 5; i++) {
            System.out.println("waiting for GracefullyShutdown: " + i);
            Thread.sleep(1000 * 1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("It will be GracefullyShutdown...");
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("It has been GracefullyShutdown...");
            }
        }));
    }
}
