# Dockerfile for Chapter 6
# Start with the slim parent image
FROM python:3.10-slim

# set the Docker working directory
WORKDIR /code

# copy from the build context directory to the Docker working directory 
COPY requirements.txt /code/

# Install the Python libraries listed in the requirements file in the Docker working directory
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

# Copy code files and database from the build context directory to the Docker working directory
COPY *.py /code/
COPY *.db /code/

# Launch the Uvicorn webserver and run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]