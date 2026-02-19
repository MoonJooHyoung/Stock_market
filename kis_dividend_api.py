import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

# 환경변수 로드
load_dotenv()

class KISDividendAPI:
    def __init__(self):
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.app_key = os.getenv('KIS_APP_KEY', '')
        self.app_secret = os.getenv('KIS_APP_SECRET', '')
        self.access_token = None
        
    def get_access_token(self):
        """한국투자증권 API 액세스 토큰 발급"""
        if not self.app_key or not self.app_secret:
            print("❌ KIS API 키가 설정되지 않았습니다.")
            return False
            
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {
            "content-type": "application/json"
        }
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get('access_token')
                print("✅ KIS API 액세스 토큰 발급 성공")
                return True
            else:
                print(f"❌ KIS API 토큰 발급 실패: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ KIS API 토큰 발급 오류: {e}")
            return False
    
    def get_dividend_schedule(self, stock_code: str):
        """종목 배당일정 조회"""
        if not self.access_token:
            if not self.get_access_token():
                return []
        
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/investor-calendar"
        headers = {
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "CTPF1002R"
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": stock_code
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ {stock_code} 배당일정 조회 성공")
                return data.get('output', [])
            else:
                print(f"❌ {stock_code} 배당일정 조회 실패: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ {stock_code} 배당일정 조회 오류: {e}")
            return []

# 대안: 간단한 배당 데이터 생성기
class SimpleDividendGenerator:
    def __init__(self):
        self.major_stocks = {
            "005930": "삼성전자",
            "000660": "SK하이닉스", 
            "035420": "NAVER",
            "035720": "카카오",
            "051910": "LG화학",
            "005380": "현대차",
            "000270": "기아",
            "005490": "POSCO",
            "105560": "KB금융",
            "055550": "신한지주",
            "086790": "하나금융지주",
            "066570": "LG전자",
            "017670": "SK텔레콤",
            "030200": "KT",
            "012330": "현대모비스",
            "068270": "셀트리온",
            "090430": "아모레퍼시픽",
            "015760": "한국전력"
        }
    
    def generate_realistic_dividend_data(self):
        """현실적인 배당 데이터 생성"""
        dividends = []
        
        # 2025년 배당 일정 생성
        dividend_dates = [
            ("2025-03-15", "2025-04-15"),  # 1분기 배당
            ("2025-06-15", "2025-07-15"),  # 2분기 배당  
            ("2025-09-15", "2025-10-15"),  # 3분기 배당
            ("2025-12-15", "2026-01-15"),  # 4분기 배당
        ]
        
        for stock_code, stock_name in self.major_stocks.items():
            for ex_date, pay_date in dividend_dates:
                # 현실적인 배당 금액 생성
                base_dividend = self._get_base_dividend(stock_name)
                dividend_per_share = base_dividend + (hash(stock_code) % 200 - 100)  # ±100원 변동
                
                dividends.append({
                    "corp_code": stock_code,
                    "corp_name": stock_name,
                    "market": "KOSPI" if stock_code.startswith("00") else "KOSDAQ",
                    "ex_dividend_date": ex_date.replace("-", ""),
                    "payment_date": pay_date.replace("-", ""),
                    "dividend_per_share": dividend_per_share,
                    "total_dividend": dividend_per_share * 1000000  # 100만주 기준
                })
        
        return dividends
    
    def _get_base_dividend(self, stock_name: str):
        """종목별 기본 배당 금액"""
        base_dividends = {
            "삼성전자": 500,
            "SK하이닉스": 300,
            "NAVER": 200,
            "카카오": 150,
            "LG화학": 400,
            "현대차": 800,
            "기아": 600,
            "POSCO": 1000,
            "KB금융": 700,
            "신한지주": 650,
            "하나금융지주": 600,
            "LG전자": 350,
            "SK텔레콤": 450,
            "KT": 400,
            "현대모비스": 550,
            "셀트리온": 250,
            "아모레퍼시픽": 300,
            "한국전력": 200
        }
        return base_dividends.get(stock_name, 300)

if __name__ == "__main__":
    print("🚀 배당 데이터 생성기 테스트")
    
    # 간단한 배당 데이터 생성기 사용
    generator = SimpleDividendGenerator()
    dividends = generator.generate_realistic_dividend_data()
    
    print(f"✅ 총 {len(dividends)}건의 배당 데이터 생성 완료")
    
    # 삼성전자 데이터만 출력
    samsung_dividends = [d for d in dividends if d["corp_name"] == "삼성전자"]
    print(f"\n📌 삼성전자 배당 데이터 ({len(samsung_dividends)}건):")
    for dividend in samsung_dividends:
        print(f"  배당락일: {dividend['ex_dividend_date']}, 배당지급일: {dividend['payment_date']}, 주당배당금: {dividend['dividend_per_share']}원")
