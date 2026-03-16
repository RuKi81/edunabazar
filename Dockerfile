FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps: GDAL, GEOS for PostGIS support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev libgeos-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

RUN python manage.py collectstatic --noinput 2>/dev/null || true

EXPOSE 8000

CMD ["gunicorn", "enb_django.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3", "--timeout", "120"]
