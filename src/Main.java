import com.distqueue.broker.Broker;
import com.distqueue.consumer.Consumer;
import com.distqueue.controller.Controller;
import com.distqueue.producer.Producer;

public class Main {
    
    public static void main(String[] args) throws InterruptedException {
        
        Controller controller = new Controller();
        Broker broker = new Broker(controller);

        broker.createTopic("TestTopic", 2, 3);

        Producer producer = new Producer(broker);
        Consumer consumer1 = new Consumer(broker, "ConsumerGroup1");
        Consumer consumer2 = new Consumer(broker, "ConsumerGroup1");

        producer.sendMessage("TestTopic", 0, "Message to partition 0".getBytes());
        producer.sendMessage("TestTopic", 1, "Message to partition 1".getBytes());

        System.out.println("Consumer 1 reading from partition 0: " + consumer1.consumeMessages("TestTopic", 0));
        System.out.println("Consumer 2 reading from partition 1: " + consumer2.consumeMessages("TestTopic", 1));
    
    }

}
