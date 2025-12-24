FROM python:3.12-slim

WORKDIR /app

# Copia requirements e installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice
COPY main.py .

# Espone la porta
EXPOSE 3000

# Avvia il server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]