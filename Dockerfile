# Use the official Python 3.10 image as the base image
FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy the pyproject.toml, requirements.txt file, and the finlogic directory into the working directory
COPY pyproject.toml requirements.txt ./
COPY finlogic/ ./finlogic/

# Install PDM
RUN pip install --no-cache-dir pdm

# Install the required dependencies using pip
RUN pip install --no-cache-dir -r requirements.txt

# Install the finlogic package using PDM
RUN pdm install --prod --no-self

# Set the PYTHONPATH environment variable to include the current working directory
ENV PYTHONPATH=/app
