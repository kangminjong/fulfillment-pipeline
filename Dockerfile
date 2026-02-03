# 가벼운 파이썬 버전 사용
FROM python:3.11-slim

# 작업 폴더 설정
WORKDIR /app

# 1. 패키지 설치부터 (캐싱 효율을 위해)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. 소스 코드는 여기서 COPY 하지 않음! 
COPY backend /app/backend
COPY web /app/web

WORKDIR /app/backend

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]