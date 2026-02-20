FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.29.0 \
    snowflake-connector-python==3.6.0 \
    pandas==2.0.3 \
    plotly==5.18.0

COPY app_main.py /app/main_app.py
COPY app_financial.py /app/financial_app.py

EXPOSE 8501 8502

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
