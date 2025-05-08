# Use an official Python base image
FROM python:3.10-slim

# Set environment variables to avoid prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# Install OS dependencies and upgrade packages
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get upgrade -y

# Install Python dependencies
RUN pip install telebot

# Copy your Python script into the container
COPY g.py /app/g.py

# Set working directory
WORKDIR /app

# Run the script
CMD ["python3", "g.py"]
