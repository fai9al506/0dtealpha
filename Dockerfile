FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

# IMPORTANT: keep YOUR existing web start command
# If your current Railway start command works, you can also keep it in Railway settings and ignore this CMD.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
