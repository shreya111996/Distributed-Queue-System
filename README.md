
# Distributed Queue System

## Getting Started

Before compiling the code in your local environment, ensure your computer has the following dependencies installed:
- **Docker**
- **Maven**

### Setting Up the Environment
Run the following commands to set up the environment:

```bash
docker build -t distqueue/consumer -f Dockerfile.consumer .
docker build -t distqueue/producer -f Dockerfile.producer .
docker build -t distqueue/controller -f Dockerfile.controller .
docker build -t distqueue/broker -f Dockerfile.broker .
mvn clean package
docker compose build
```

### Running the Program
To start the program, use the following command:
```bash
docker compose up
```

### Building and Running the Frontend
To see the frontend build:
1. Navigate to the `distqueue-ui` folder:
   ```bash
   cd distqueue-ui
   ```
2. Install the dependencies:
   ```bash
   npm install
   ```
3. Start the frontend:
   ```bash
   npm start
   ```

## Folder Structure

The workspace contains the following major folders:
- **`src`**: The distributed queue server
- **`distqueue-ui`**: The frontend that retrieves information from the backend
