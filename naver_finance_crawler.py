import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import random

class NaverFinanceCrawler:
    def __init__(self):
        self.base_url = "https://finance.naver.com/item/dividend.nhn"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
    def get_dividend_schedule(self, stock_code: str):

        url = f"{self.base_url}?code={stock_code}"
        
        try:
            print(f"🔍 {stock_code} 배당일정 조회 중...")
            res = requests.get(url, headers=self.headers, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # 테이블 찾기
            table = soup.find("table", {"class": "tbl"})
            if not table:
                print(f"❌ {stock_code} 배당일정 테이블을 찾을 수 없습니다.")
                return []

            rows = table.find_all("tr")[1:]  # 헤더 제외
            schedule = []

            for row in rows:
                cols = [col.get_text(strip=True) for col in row.find_all("td")]
                if len(cols) < 6:
                    continue
                    
                # 배당 데이터 파싱
                dividend_data = {
                    "결산기준일": cols[0],
                    "현금배당": cols[1],
                    "현금배당률": cols[2],
                    "주식배당률": cols[3],
                    "배당기산일": cols[4],
                    "배당지급일": cols[5],
                }
                
                # 배당락일 계산 (배당기산일 - 1 거래일)
                ex_dividend_date = self._calculate_ex_dividend_date(cols[4])
                if ex_dividend_date:
                    dividend_data["배당락일"] = ex_dividend_date
                
                schedule.append(dividend_data)
                
            print(f"✅ {stock_code} 배당일정 {len(schedule)}건 조회 완료")
            return schedule

        except Exception as e:
            print(f"❌ {stock_code} 배당일정 조회 실패: {e}")
            return []
    
    def _calculate_ex_dividend_date(self, base_date_str: str):
        """
        배당기산일을 기준으로 배당락일 계산
        :param base_date_str: 배당기산일 (YYYY.MM.DD 형식)
        :return: 배당락일 (YYYYMMDD 형식)
        """
        try:
            # 날짜 파싱
            base_date = datetime.strptime(base_date_str, "%Y.%m.%d")
            
            # 배당락일 = 배당기산일 - 1 거래일
            ex_dividend_date = base_date - timedelta(days=1)
            
            # 주말인 경우 금요일로 조정
            if ex_dividend_date.weekday() == 6:  # 일요일
                ex_dividend_date -= timedelta(days=2)
            elif ex_dividend_date.weekday() == 5:  # 토요일
                ex_dividend_date -= timedelta(days=1)
            
            return ex_dividend_date.strftime("%Y%m%d")
            
        except Exception as e:
            print(f"❌ 배당락일 계산 실패: {e}")
            return None
    
    def get_multiple_stocks_dividend(self, stock_codes: list):
        """
        여러 종목의 배당일정을 일괄 조회
        :param stock_codes: 종목 코드 리스트
        :return: 전체 배당 일정 리스트
        """
        all_dividends = []
        
        for i, stock_code in enumerate(stock_codes):
            print(f"\n📊 진행률: {i+1}/{len(stock_codes)} - {stock_code}")
            
            dividends = self.get_dividend_schedule(stock_code)
            if dividends:
                # 종목 코드 추가
                for dividend in dividends:
                    dividend["종목코드"] = stock_code
                all_dividends.extend(dividends)
            
            # 요청 간격 조절 (너무 빠른 요청 방지)
            if i < len(stock_codes) - 1:
                time.sleep(random.uniform(1, 2))
        
        return all_dividends

# 주요 종목 코드 리스트
MAJOR_STOCKS = {
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

if __name__ == "__main__":
    crawler = NaverFinanceCrawler()
    
    # 삼성전자 테스트
    print("🧪 삼성전자 배당일정 테스트")
    samsung_data = crawler.get_dividend_schedule("005930")
    
    if samsung_data:
        print("\n📌 삼성전자 배당일정 결과:")
        for item in samsung_data:
            print(f"  {item}")
    else:
        print("❌ 삼성전자 배당일정 조회 실패")
    
    # 전체 주요 종목 조회 (선택사항)
    # print("\n🚀 전체 주요 종목 배당일정 조회")
    # all_dividends = crawler.get_multiple_stocks_dividend(list(MAJOR_STOCKS.keys()))
    # print(f"\n✅ 총 {len(all_dividends)}건의 배당일정 조회 완료")
