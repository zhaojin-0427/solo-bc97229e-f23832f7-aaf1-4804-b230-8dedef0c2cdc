FROM python:3.12-slim

WORKDIR /srv
COPY server.py .
COPY site ./site

EXPOSE 8080
CMD ["python", "server.py"]
