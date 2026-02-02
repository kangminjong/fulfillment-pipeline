# 가벼운 파이썬 버전 사용
FROM python:3.9-slim

# 작업 폴더 설정
WORKDIR /app

# 1. 패키지 설치부터 (캐싱 효율을 위해)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. 소스 코드는 여기서 COPY 하지 않음! 
# (docker-compose의 volumes 기능을 써서 마운트 할 것이기 때문)

# 기본 명령어 (docker-compose에서 덮어쓰지만 혹시 모르니 설정)
CMD ["tail", "-f", "/dev/null"]