# Use an official Python base image
FROM python:3.10-slim

# Set environment variable to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update packages, install sudo and ffmpeg
RUN apt-get update && \
    apt-get install -y sudo ffmpeg && \
    apt-get upgrade -y

# Optional: Create a user and give sudo access (if you really need "sudo")
RUN useradd -m myuser && echo "myuser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install Python dependencies
RUN pip install telebot

# Copy your script into the image
COPY g.py /app/g.py

# Set working directory
WORKDIR /app

# Run the script
CMD ["python3", "g.py"]
