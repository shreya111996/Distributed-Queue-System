package com.distqueue.protocols;

import com.distqueue.broker.Broker;
import com.distqueue.core.Message;

import java.time.Instant;
import java.util.List;
import java.util.Random;

public class GossipProtocol {

    private final List<Broker> peers;  // List of all brokers (peers) in the gossip network
    private final Random random = new Random();

    public GossipProtocol(List<Broker> peers) {
        this.peers = peers;  // Accept a list of brokers as peers
    }

    // Start the gossip process
    public void startGossip() {
        while (true) {
            try {
                // Randomly pick a peer and start the gossip exchange (push-pull)
                Broker peer = getRandomPeer();
                gossipPush(peer);  // Push state to a random peer
                gossipPull();  // Pull state (no argument needed here)
                Thread.sleep(5000); // Sleep for 5 seconds before the next gossip round
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Public method to push gossip message to a randomly selected peer
    public void gossipPush(Broker peer) {
        Message gossipMessage = new Message("Broker-" + peer.getId(), "Metadata updates", Instant.now());
        // Send the gossip message to the peer
        peer.receivePushGossip(gossipMessage);
    }

    // Public method to pull gossip message from a random peer (no arguments)
    public Message gossipPull() {
        // Simulate pulling gossip from a random peer
        return new Message("Broker-" + random.nextInt(100), "Gossip Metadata", Instant.now());
    }

    // Helper function to randomly pick a peer
    private Broker getRandomPeer() {
        int index = random.nextInt(peers.size());
        return peers.get(index);
    }
}
