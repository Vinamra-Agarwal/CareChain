FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p src/infrastructure/database src/infrastructure/messaging \
    src/domain src/iot src/edge src/application src/presentation

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app
ENV DATABASE_URL=postgresql+asyncpg://carechain:password@postgres:5432/carechain_db
ENV REDIS_URL=redis://redis:6379

# Run the application
CMD ["python", "main.py"]
