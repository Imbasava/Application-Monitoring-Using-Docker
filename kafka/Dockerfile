# Use official Node.js image as the base image
FROM node:16

# Set the working directory in the container
WORKDIR /usr/src/app/kafka

# Copy package.json and package-lock.json to install dependencies
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the Kafka producer code to the container
COPY . .

# Expose Kafka producer port (Kafka default is 9093 for connections)
EXPOSE 9093

# Command to start Kafka producer
CMD ["node", "producer.js"]
