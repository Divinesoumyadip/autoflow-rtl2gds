FROM python:3.12-slim

WORKDIR /app

# Install C++ build tools for DAG engine compilation
RUN apt-get update && apt-get install -y \
    g++ cmake make \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
