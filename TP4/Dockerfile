FROM python:3.9-slim

RUN pip install duckdb pandas pyarrow numpy streamlit

WORKDIR /app

# COPY generate_data.py app/.
# COPY data/. app/data/.

COPY . . 

# CMD ["python", "generate_data.py"]
# CMD python generate_data.py && tail -f /dev/null

RUN python generate_data.py

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
