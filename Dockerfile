FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY app.py .

# Create data directory
RUN mkdir -p data

# Expose port
EXPOSE 8000

# Run
CMD ["python", "app.py"]
