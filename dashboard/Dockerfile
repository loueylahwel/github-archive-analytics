FROM python:3.11-slim

WORKDIR /opt/app

# Only dependencies are baked into the image — the code is mounted at runtime
# via the compose volume (repo root -> /opt/app), so edits reflect live.
COPY requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
