Getting Started
Before compiling the code under local environment, the computer needs to have the below dependencies:
• docker
• maven

After, to set up the environment, run the following commands:
• docker build -t distqueue/consumer -f Dockerfile.consumer .
• docker build -t distqueue/producer -f Dockerfile.producer .
• docker build -t distqueue/controller -f Dockerfile.controller .
• docker build -t distqueue/broker -f Dockerfile.broker .
• mvn clean package
• docker compose build Then, to run the program, do:
• docker compose up

After, to see the frontend build, do:
• cd distqueue-ui
• npm install
• npm start

Folder Structure
The workspace contains the major folders below:
• src: The distributed queue server
• distqueue-ui: The frontend retrieving information from the
backend
