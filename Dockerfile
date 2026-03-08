# Используем текущую стабильную версию 3.14
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Зависимости для FastAPI и работы с t-strings
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install ruff

COPY . .

EXPOSE 8000

# CMD ["fastapi", "dev", "main.py", "--host", "0.0.0.0", "--port", "8000"]
CMD ["tail", "-f", "/dev/null"]
