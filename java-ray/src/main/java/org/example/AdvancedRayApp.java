package org.example;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AdvancedRayApp {

    // 1. 普通远程函数（无状态）
    public static class SimpleFunctions {
        public static int add(int a, int b) {
            System.out.println("Adding " + a + " + " + b);
            return a + b;
        }

        public static String greet(String name) {
            System.out.println("Greeting " + name);
            return "Hello, " + name + "!";
        }

        public static List<String> processList(List<String> items) {
            System.out.println("Processing list: " + items);
            return items.stream()
                    .map(String::toUpperCase)
                    .collect(Collectors.toList());
        }
    }

    // 2. Actor（有状态）
    public static class Counter {
        private int value = 0;

        public int increment() {
            value++;
            return value;
        }

        public int getValue() {
            return value;
        }

        public int add(int amount) {
            value += amount;
            return value;
        }
    }

    // 3. 有参数的Actor
    public static class ConfigurableGreeter {
        private final String prefix;

        public ConfigurableGreeter(String prefix) {
            this.prefix = prefix;
        }

        public String greet(String name) {
            return prefix + ", " + name + "!";
        }
    }

    // 4. 任务依赖
    public static class TaskChain {
        public static int step1() {
            return 10;
        }

        public static int step2(int input) {
            return input * 2;
        }

        public static int step3(int input1, int input2) {
            return input1 + input2;
        }
    }

    // 5. 异步任务处理
    public static class AsyncProcessor {
        public static String process(String input, int delayMillis) {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return input.toUpperCase();
        }
    }

    public static void main(String[] args) {
        System.setProperty("ray.address", "localhost:6379");
        System.out.println("ray.address = " + System.getProperty("ray.address"));

        // 初始化Ray
        Ray.init();

        System.out.println("\n===== 1. 基本远程函数调用 =====");
        basicFunctionCalls();

        System.out.println("\n===== 2. Actor使用 =====");
        actorUsage();

        System.out.println("\n===== 3. 任务链和依赖 =====");
        taskChaining();

        System.out.println("\n===== 4. 异步处理和批量调用 =====");
        asyncProcessing();

        System.out.println("\n===== 6. 获取集群信息 =====");
        clusterInfo();

        // 关闭Ray
        Ray.shutdown();
        System.out.println("\n所有任务完成!");
    }

    private static void basicFunctionCalls() {
        // 简单函数调用
        ObjectRef<Integer> sumResult = Ray.task(SimpleFunctions::add, 5, 7).remote();
        System.out.println("5 + 7 = " + sumResult.get());

        // 带返回值的函数
        ObjectRef<String> greeting = Ray.task(SimpleFunctions::greet, "Ray").remote();
        System.out.println(greeting.get());

        // 处理列表
        List<String> names = Arrays.asList("Alice", "Bob", "Charlie");
        ObjectRef<List<String>> processedList = Ray.task(SimpleFunctions::processList, names).remote();
        System.out.println("Processed list: " + processedList.get());
    }

    private static void actorUsage() {
        // 创建Actor
        ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

        // 调用Actor方法
        counter.task(Counter::increment).remote();
        counter.task(Counter::increment).remote();
        counter.task(Counter::increment).remote();

        // 获取Actor状态
        ObjectRef<Integer> currentValue = counter.task(Counter::getValue).remote();
        System.out.println("Counter value: " + currentValue.get());

        // 带参数的调用
        counter.task(Counter::add, 5).remote();
        System.out.println("After adding 5: " + counter.task(Counter::getValue).remote().get());

        // 创建带参数的Actor
        ActorHandle<ConfigurableGreeter> greeter = Ray.actor(ConfigurableGreeter::new, "Bonjour").remote();
        System.out.println(greeter.task(ConfigurableGreeter::greet, "Élise").remote().get());

        // 创建多个Actor
        List<ActorHandle<Counter>> counters = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            counters.add(Ray.actor(Counter::new).remote());
        }

        // 批量调用
        List<ObjectRef<Integer>> incrementResults = new ArrayList<>();
        for (ActorHandle<Counter> c : counters) {
            incrementResults.add(c.task(Counter::increment).remote());
        }

        // 获取所有结果
        List<Integer> values = Ray.get(incrementResults);
        System.out.println("Counters values: " + values);
    }

    private static void taskChaining() {
        // 任务链：一个任务的输出是另一个任务的输入
        ObjectRef<Integer> step1 = Ray.task(TaskChain::step1).remote();
        ObjectRef<Integer> step2 = Ray.task(TaskChain::step2, step1).remote();

        // 同时使用多个任务的输出
        ObjectRef<Integer> step3 = Ray.task(TaskChain::step3, step1, step2).remote();

        System.out.println("Step1 result: " + step1.get());
        System.out.println("Step2 result: " + step2.get());
        System.out.println("Step3 result: " + step3.get());
    }

    private static void asyncProcessing() {
        List<String> items = Arrays.asList("apple", "banana", "cherry", "date", "elderberry");
        List<ObjectRef<String>> asyncResults = new ArrayList<>();

        // 启动异步任务
        for (int i = 0; i < items.size(); i++) {
            int delay = 500 * (i + 1); // 递增的延迟
            asyncResults.add(Ray.task(AsyncProcessor::process, items.get(i), delay).remote());
            System.out.println("Started processing: " + items.get(i) + " with delay " + delay + "ms");
        }

        System.out.println("All tasks started. Waiting for results...");

        // 等待部分任务完成
        List<ObjectRef<String>> ready = Ray.wait(asyncResults, items.size(), 2000).getReady();
        System.out.println("Immediately ready results: " + ready.size());

        // 获取所有结果（带超时）
        try {
            List<String> results = Ray.get(asyncResults, 3000);
            System.out.println("All results: " + results);
        } catch (Exception e) {
            System.out.println("Timeout occurred, getting partial results");
            List<ObjectRef<String>> partialReady = Ray.wait(asyncResults, items.size(), 0).getReady();
            System.out.println("Partial results: " + Ray.get(partialReady));
        }
    }

    private static void clusterInfo() {
        // 获取当前任务/actor信息
        System.out.println("Current task ID: " + Ray.getRuntimeContext().getCurrentTaskId());
        System.out.println("Current job ID: " + Ray.getRuntimeContext().getCurrentJobId());
    }
}