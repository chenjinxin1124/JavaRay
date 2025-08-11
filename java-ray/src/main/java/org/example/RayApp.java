package org.example;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;

public class RayApp {

    public static class Counter {
        private int value = 0;

        public int increment() {
            return ++value;
        }
    }

    public static void main(String[] args) {
        // 配置连接参数
        System.setProperty("ray.address", "localhost:6379");
//        System.setProperty("ray.address", "10.244.0.28:6379");
//        System.setProperty("ray.cluster-id", "1234567890abcdef"); // 替换为实际集群ID
//        System.setProperty("ray.raylet.config.namespace", "default");
        System.setProperty("ray.logging.level", "DEBUG");
        System.setProperty("ray.gcs.connect.retries", "3");
        System.setProperty("ray.gcs.connect.timeout", "10000");

        // 等待集群准备
        try {
            System.out.println("等待集群准备...");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("ray.address = " + System.getProperty("ray.address"));
        // 初始化 Ray
        Ray.init();

        // 创建 Actor
        ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

        // 远程调用并获取结果
        ObjectRef<Integer> result = counter.task(Counter::increment).remote();
        System.out.println("Result: " + result.get());

        // 关闭连接
        Ray.shutdown();
    }
}