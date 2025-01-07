# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./python /app

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

# Run app.py when the container launches
CMD ["python", "integration_tests/runner.py"]
