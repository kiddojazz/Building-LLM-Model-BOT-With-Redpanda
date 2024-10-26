# Building-LLM-Model-BOT-With-Redpanda


# Introduction

We are in the era of fast rapid information being shared and transferred in real time. With the help of the Large Language Model human intelligence and performance have improved more than ever before. Ability to solve more complex problems faster and more precisely.

# Business Proposition

With streaming technology being on the rise we plan to build an LLM chatbot and deploy it to Telegram where users can interact with the solution in real time and get precise responses.

# Solution Architecture

The solution will be broken don

### Step 1: Set up Docker-Compose.yml

This will contain all the different services needed for this project. As we plan to store the data in ElasticSearch and visualize it with Kibana.

### Step 2: Create a Cluster and Topic in the Redpanda Site

Create the cluster needed for the project and create a topic to which all the data will be sent.

### Step 3: Create both Producer Script and Consumer Script

As this project will be using Python to send data to Redpanda topic consumes the data and sends it to ElasticSearch and Azure Data Lake Gen 2.

### Step 4: Data Transformation and Standardization

The stored data in the Azure storage account are then transformed and prepare for training purposes.

### Step 5: Create a LLM Chatbot with the Curated Data
