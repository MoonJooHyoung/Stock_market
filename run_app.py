"""
배당 달력 애플리케이션 실행 스크립트
"""

import os
from dotenv import load_dotenv
from web_server import app, init_collector

def main():
    """메인 실행 함수"""
    # 환경변수 로드
    load_dotenv()
    
    # API 키 확인
    dart_api_key = os.getenv('DART_API_KEY')
    holiday_api_key = os.getenv('HOLIDAY_API_KEY')
    
    if not dart_api_key:
        print("❌ DART_API_KEY가 설정되지 않았습니다.")
        print("env_example.txt를 .env로 복사하고 API 키를 설정해주세요.")
        return
    
    if not holiday_api_key:
        print("❌ HOLIDAY_API_KEY가 설정되지 않았습니다.")
        print("공공데이터포털에서 API 키를 발급받아 설정해주세요.")
        return
    
    print("🚀 배당 달력 애플리케이션을 시작합니다...")
    print(f"📊 DART API 키: {dart_api_key[:10]}...")
    print(f"📅 공휴일 API 키: {holiday_api_key[:10]}...")
    print("🌐 웹 서버: http://localhost:5000")
    print("📱 브라우저에서 위 주소로 접속하세요.")
    
    # 데이터 수집기 초기화
    init_collector()
    
    # 웹 서버 실행
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    main()
