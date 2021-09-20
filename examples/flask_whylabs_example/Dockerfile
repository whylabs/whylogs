ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}

# Create a working directory.
RUN mkdir /app
WORKDIR /app
# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy the code
COPY ./ /app

CMD [ "gunicorn", "--workers=5", "--bind=0.0.0.0:5000", "--threads=1", "--reload", "autoapp:app"]