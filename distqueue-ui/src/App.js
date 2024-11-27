import React from 'react';
import { BrowserRouter as Router, Route, Switch } from 'react-router-dom';
import Dashboard from './components/Dashboard';
import Brokers from './components/Brokers';
import Topics from './components/Topics';
import Messages from './components/Messages';

function App() {
  return (
    <Router>
      <div>
        <Switch>
          <Route path="/" exact component={Dashboard} />
          <Route path="/brokers" component={Brokers} />
          <Route path="/topics" component={Topics} />
          <Route path="/messages" component={Messages} />
        </Switch>
      </div>
    </Router>
  );
}

export default App;