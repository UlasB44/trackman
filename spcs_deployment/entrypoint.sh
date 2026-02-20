#!/bin/bash
streamlit run /app/main_app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true &
streamlit run /app/financial_app.py --server.port 8502 --server.address 0.0.0.0 --server.headless true &
wait
