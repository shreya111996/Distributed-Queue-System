import React, { useState, useEffect } from 'react';

const Dashboard = () => {
  const [brokers, setBrokers] = useState([]);
  const [activeBrokers, setActiveBrokers] = useState([]);
  const [leaderBroker, setLeaderBroker] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Broker information with external ports from docker-compose.yml
  const brokerList = [
    { id: 1, name: 'Broker 1', url: 'http://localhost:8085/health' },
    { id: 2, name: 'Broker 2', url: 'http://localhost:8086/health' },
    { id: 3, name: 'Broker 3', url: 'http://localhost:8087/health' },
  ];

  // Fetch the health status of each broker
  const fetchBrokers = async () => {
    try {
      setLoading(true);
      const brokerPromises = brokerList.map(async (broker) => {
        try {
          const response = await fetch(broker.url);
          if (response.ok) {
            return { ...broker, status: 'Healthy' };
          }
          return { ...broker, status: 'Unhealthy' };
        } catch (error) {
          return { ...broker, status: 'Unreachable' };
        }
      });

      const brokerStatuses = await Promise.all(brokerPromises);
      setBrokers(brokerStatuses);
    } catch (err) {
      setError('Failed to fetch brokers data');
    } finally {
      setLoading(false);
    }
  };

  // Fetch active brokers
  const fetchActiveBrokers = async () => {
    try {
      const response = await fetch('http://localhost:8090/brokers/active'); // Update this with correct endpoint
      if (response.ok) {
        const activeBrokersData = await response.json();
        setActiveBrokers(activeBrokersData);
      }
    } catch (error) {
      console.error('Error fetching active brokers:', error);
    }
  };

  // Fetch the leader broker
  const fetchLeaderBroker = async () => {
    try {
      const response = await fetch('http://localhost:8085/brokers/leader?topic=TestTopic'); // Update this with correct endpoint
      if (response.ok) {
        const leader = await response.json();
        setLeaderBroker(leader);
      }
    } catch (error) {
      console.error('Error fetching leader broker:', error);
    }
  };

  // UseEffect hooks to fetch data when the component mounts
  useEffect(() => {
    fetchBrokers();
    fetchActiveBrokers();
    fetchLeaderBroker();
  }, []);

  return (
    <div>
      <h1>Dashboard</h1>
      <p>Welcome to the Distributed Queue System Dashboard</p>

      {loading && <p>Loading...</p>}
      {error && <p>Error: {error}</p>}

      {!loading && !error && (
        <div>
          {/* Health Status Section */}
          <h2>Brokers Health</h2>
          <ul>
            {brokers.map((broker) => (
              <li key={broker.id}>
                {broker.name} -{' '}
                {broker.status === 'Healthy' ? '✅ Healthy' : broker.status === 'Unhealthy' ? '❌ Unhealthy' : '⚠️ Unreachable'}
              </li>
            ))}
          </ul>

          {/* Active Brokers Section */}
          <h2>Active Brokers</h2>
          <ul>
            {activeBrokers.length > 0 ? (
              activeBrokers.map((broker) => (
                <li key={broker.brokerId}>
                  {broker.host}:{broker.port} (Broker ID: {broker.brokerId})
                </li>
              ))
            ) : (
              <p>No active brokers found.</p>
            )}
          </ul>

          {/* Leader Broker Section */}
          <h2>Leader Broker</h2>
          {leaderBroker ? (
            <p>{leaderBroker.leaderId} is the current leader broker.</p>
          ) : (
            <p>No leader broker found.</p>
          )}
        </div>
      )}
    </div>
  );
};


export default Dashboard;