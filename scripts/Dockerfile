FROM python:3.11-slim

WORKDIR /app

# Install required libraries
RUN pip install --no-cache-dir streamlit pandas psycopg2-binary plotly sqlalchemy dotenv


# Copy script
COPY dashboard.py .

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]