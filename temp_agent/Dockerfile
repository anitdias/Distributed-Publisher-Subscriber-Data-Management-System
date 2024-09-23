# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app


# Copy the rest of the application code
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Run the script
CMD ["python", "main.py"]
