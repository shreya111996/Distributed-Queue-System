Getting Started

Before compiling the code in the local environment, ensure that your computer has the following dependencies installed:

Docker

Maven

Setting Up the Environment

Run the following commands to set up the environment:

Build the Docker images for the various components:

docker build -t distqueue/consumer -f Dockerfile.consumer .
docker build -t distqueue/producer -f Dockerfile.producer .
docker build -t distqueue/controller -f Dockerfile.controller .
docker build -t distqueue/broker -f Dockerfile.broker .

Package the Maven project:

mvn clean package

Build the Docker Compose setup:

docker compose build

Running the Program

Start the application by running:

docker compose up

Viewing the Frontend

To set up and view the frontend build, follow these steps:

Navigate to the distqueue-ui directory:

cd distqueue-ui

Install the dependencies:

npm install

Start the frontend:

npm start

Folder Structure

The workspace contains the following major folders:

src: The distributed queue server

distqueue-ui: The frontend, which retrieves information from the backend
