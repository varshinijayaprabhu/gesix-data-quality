# Use Python 3.10 slim image
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs

# Set up a new user named "user" with user ID 1000
RUN useradd -m -u 1000 user
USER user

# Set home to the user's home directory
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

# Copy the current directory contents into the container at $HOME/app setting the owner to the user
COPY --chown=user . $HOME/app

# Build the frontend
WORKDIR $HOME/app/frontend
RUN npm install
RUN npm run build

# Install backend dependencies
WORKDIR $HOME/app/backend
RUN pip install --no-cache-dir -r requirements.txt

# Create temp directories if needed
RUN mkdir -p data/temp

# Expose the port Hugging Face Spaces expects
EXPOSE 7860
ENV PORT=7860
ENV HOST=0.0.0.0

# Command to run the FastApi application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]
