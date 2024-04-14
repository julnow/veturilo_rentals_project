# Use an official Python runtime as a parent image
FROM python:3.10

# Set environment variables
ENV PYSPARK_PYTHON /usr/local/bin/python3.10
ENV PYSPARK_DRIVER_PYTHON /usr/local/bin/python3.10

# Install required Python packages
RUN pip install pyspark

# Set the working directory in the container
WORKDIR /app

# Copy the entire current directory into the container at /app
COPY . /app

# Define the entry point for the container
CMD ["python", "your_script.py"]
