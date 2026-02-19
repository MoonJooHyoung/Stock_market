"""
Investing.com 배당 캘린더 크롤러
미국 및 한국 기업의 실제 배당 일정 데이터를 수집합니다.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import json
import sqlite3
from typing import Dict, List, Optional
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InvestingDividendCrawler:
    """Investing.com 배당 데이터 크롤러"""
    
    def __init__(self, db_path: str = "dividend_calendar.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.init_database()
    
    def init_database(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 기존 테이블이 있는지 확인
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dividend_events'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            # 크롤링된 배당 데이터 테이블 생성
            cursor.execute('''
                CREATE TABLE dividend_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    country TEXT NOT NULL,
                    ex_dividend_date TEXT NOT NULL,
                    payment_date TEXT,
                    dividend_amount REAL,
                    dividend_yield REAL,
                    currency TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 인덱스 생성
            cursor.execute('CREATE INDEX idx_ex_dividend_date ON dividend_events(ex_dividend_date)')
            cursor.execute('CREATE INDEX idx_company ON dividend_events(company_name)')
            cursor.execute('CREATE INDEX idx_ticker ON dividend_events(ticker)')
        else:
            # 기존 테이블에 컬럼이 있는지 확인하고 없으면 추가
            cursor.execute("PRAGMA table_info(dividend_events)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'company_name' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN company_name TEXT')
            if 'ticker' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN ticker TEXT')
            if 'country' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN country TEXT')
            if 'ex_dividend_date' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN ex_dividend_date TEXT')
            if 'payment_date' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN payment_date TEXT')
            if 'dividend_amount' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN dividend_amount REAL')
            if 'dividend_yield' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN dividend_yield REAL')
            if 'currency' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN currency TEXT')
            if 'created_at' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            if 'updated_at' not in columns:
                cursor.execute('ALTER TABLE dividend_events ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            
            # 인덱스 생성 (없는 경우에만)
            try:
                cursor.execute('CREATE INDEX idx_ex_dividend_date ON dividend_events(ex_dividend_date)')
            except:
                pass
            try:
                cursor.execute('CREATE INDEX idx_company ON dividend_events(company_name)')
            except:
                pass
            try:
                cursor.execute('CREATE INDEX idx_ticker ON dividend_events(ticker)')
            except:
                pass
        
        conn.commit()
        conn.close()
    
    def get_dividend_calendar_data(self, start_date: str, end_date: str) -> List[Dict]:
        """특정 기간의 배당 캘린더 데이터 수집"""
        try:
            # Investing.com 배당 캘린더 URL
            base_url = "https://www.investing.com/dividends-calendar/"
            
            # 날짜 범위 설정
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_dividends = []
            
            # 각 월별로 데이터 수집
            current_date = start_dt
            while current_date <= end_dt:
                month_data = self._scrape_month_data(current_date.year, current_date.month)
                all_dividends.extend(month_data)
                
                # 다음 달로 이동
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
                
                # 요청 간격 조절 (차단 방지)
                time.sleep(random.uniform(1, 3))
            
            return all_dividends
            
        except Exception as e:
            logger.error(f"배당 데이터 수집 중 오류 발생: {e}")
            return []
    
    def _scrape_month_data(self, year: int, month: int) -> List[Dict]:
        """특정 월의 배당 데이터 스크래핑"""
        try:
            # Investing.com API 엔드포인트 (실제 구조에 맞게 조정 필요)
            url = f"https://www.investing.com/dividends-calendar/Service/getCalendarFilteredData"
            
            # 요청 데이터
            data = {
                'country[]': ['5', '39'],  # 미국(5), 한국(39)
                'dateFrom': f"{year}-{month:02d}-01",
                'dateTo': f"{year}-{month:02d}-{self._get_last_day_of_month(year, month):02d}",
                'currentTab': 'today',
                'limit_from': '0'
            }
            
            response = self.session.post(url, data=data)
            
            if response.status_code == 200:
                # JSON 응답 파싱
                result = response.json()
                return self._parse_dividend_data(result)
            else:
                logger.warning(f"API 요청 실패: {response.status_code}")
                return self._scrape_fallback_data(year, month)
                
        except Exception as e:
            logger.error(f"월별 데이터 스크래핑 오류: {e}")
            return self._scrape_fallback_data(year, month)
    
    def _scrape_fallback_data(self, year: int, month: int) -> List[Dict]:
        """대체 방법으로 데이터 수집 (HTML 스크래핑)"""
        try:
            # 실제 Investing.com 배당 캘린더 URL
            url = f"https://www.investing.com/dividends-calendar/"
            
            # 요청 헤더 개선
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = self.session.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            dividends = []
            
            # 실제 테이블 구조에 맞게 수정
            table = soup.find('table', {'id': 'dividendsCalendarData'})
            if not table:
                # 다른 가능한 테이블 ID들 시도
                table = soup.find('table', class_='genTbl')
            
            if table:
                rows = table.find('tbody')
                if rows:
                    rows = rows.find_all('tr')
                else:
                    rows = table.find_all('tr')[1:]  # 헤더 제외
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 5:
                        try:
                            # 실제 Investing.com 테이블 구조에 맞게 수정
                            company_name = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                            ticker = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                            ex_date = cols[3].get_text(strip=True) if len(cols) > 3 else ''
                            pay_date = cols[4].get_text(strip=True) if len(cols) > 4 else ''
                            amount = cols[5].get_text(strip=True) if len(cols) > 5 else '0'
                            yield_val = cols[6].get_text(strip=True) if len(cols) > 6 else '0'
                            
                            # 국가 구분 (실제 데이터에 따라)
                            country = 'US'  # 기본값
                            currency = '$'
                            
                            # 한국 기업인지 확인 (티커나 회사명으로)
                            if any(kr_indicator in company_name.lower() for kr_indicator in ['samsung', 'lg', 'sk', 'hyundai', 'kia', 'posco']):
                                country = 'KR'
                                currency = '₩'
                            elif ticker.isdigit() and len(ticker) == 6:  # 한국 주식 코드
                                country = 'KR'
                                currency = '₩'
                            
                            dividend_data = {
                                'company_name': company_name,
                                'ticker': ticker,
                                'country': country,
                                'ex_dividend_date': self._parse_date(ex_date),
                                'payment_date': self._parse_date(pay_date),
                                'dividend_amount': self._parse_amount(amount),
                                'dividend_yield': self._parse_yield(yield_val),
                                'currency': currency
                            }
                            
                            # 유효한 데이터만 추가
                            if dividend_data['company_name'] and dividend_data['ex_dividend_date']:
                                dividends.append(dividend_data)
                                
                        except Exception as e:
                            logger.warning(f"행 데이터 파싱 오류: {e}")
                            continue
            
            logger.info(f"수집된 배당 데이터: {len(dividends)}개")
            return dividends
            
        except Exception as e:
            logger.error(f"대체 데이터 수집 오류: {e}")
            return []
    
    def _parse_dividend_data(self, api_response: Dict) -> List[Dict]:
        """API 응답에서 배당 데이터 파싱"""
        dividends = []
        
        try:
            if 'data' in api_response:
                for item in api_response['data']:
                    dividend_data = {
                        'company_name': item.get('name', ''),
                        'ticker': item.get('ticker', ''),
                        'country': 'US' if item.get('country_id') == 5 else 'KR',
                        'ex_dividend_date': item.get('ex_date', ''),
                        'payment_date': item.get('pay_date', ''),
                        'dividend_amount': self._parse_amount(item.get('amount', '0')),
                        'dividend_yield': self._parse_yield(item.get('yield', '0')),
                        'currency': '$' if item.get('country_id') == 5 else '₩'
                    }
                    dividends.append(dividend_data)
        except Exception as e:
            logger.error(f"API 응답 파싱 오류: {e}")
        
        return dividends
    
    def _parse_date(self, date_str: str) -> str:
        """날짜 문자열 파싱"""
        try:
            if not date_str or date_str == '-' or date_str.strip() == '':
                return ''
            
            # 공백 제거
            date_str = date_str.strip()
            
            # 다양한 날짜 형식 처리
            date_formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%b %d, %Y',
                '%d %b %Y',
                '%B %d, %Y',
                '%d %B %Y',
                '%m-%d-%Y',
                '%d-%m-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # 월 이름 매핑
            month_mapping = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            }
            
            # "Sep 23, 2025" 형식 처리
            for month_name, month_num in month_mapping.items():
                if month_name in date_str:
                    parts = date_str.replace(',', '').split()
                    if len(parts) >= 3:
                        try:
                            day = parts[1].zfill(2)
                            year = parts[2]
                            return f"{year}-{month_num}-{day}"
                        except:
                            continue
            
            return ''
        except Exception as e:
            logger.warning(f"날짜 파싱 오류: {date_str} - {e}")
            return ''
    
    def _parse_amount(self, amount_str: str) -> float:
        """금액 문자열 파싱"""
        try:
            if not amount_str or amount_str == '-':
                return 0.0
            
            # 숫자만 추출
            import re
            numbers = re.findall(r'[\d.]+', amount_str.replace(',', ''))
            if numbers:
                return float(numbers[0])
            return 0.0
        except:
            return 0.0
    
    def _parse_yield(self, yield_str: str) -> float:
        """수익률 문자열 파싱"""
        try:
            if not yield_str or yield_str == '-':
                return 0.0
            
            # % 기호 제거 후 숫자 추출
            import re
            numbers = re.findall(r'[\d.]+', yield_str.replace('%', ''))
            if numbers:
                return float(numbers[0])
            return 0.0
        except:
            return 0.0
    
    def _get_last_day_of_month(self, year: int, month: int) -> int:
        """월의 마지막 날 반환"""
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        last_day = next_month - timedelta(days=1)
        return last_day.day
    
    def _get_market_type(self, ticker: str, country: str) -> str:
        """티커와 국가를 기반으로 시장 타입 반환"""
        if country == 'KR':
            return 'KOSPI'  # 한국은 KOSPI로 통일
        
        # 미국 주식의 경우 티커 길이와 패턴으로 구분
        if country == 'US':
            ticker = ticker.upper()
            
            # 주요 NASDAQ 종목들 (일반적으로 4글자 이하)
            nasdaq_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM']
            if ticker in nasdaq_tickers or len(ticker) <= 4:
                return 'NASDAQ'
            else:
                return 'NYSE'
        
        # 기타 국가들
        country_markets = {
            'JP': 'TSE',  # 도쿄증권거래소
            'CN': 'SSE',  # 상하이증권거래소
            'HK': 'HKEX', # 홍콩거래소
            'GB': 'LSE',  # 런던증권거래소
            'DE': 'FSE',  # 프랑크푸르트증권거래소
            'FR': 'EPA',  # 유로넥스트 파리
        }
        
        return country_markets.get(country, 'OTHER')
    
    def save_dividend_data(self, dividends: List[Dict]):
        """배당 데이터를 데이터베이스에 저장"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for dividend in dividends:
                # 중복 확인
                cursor.execute('''
                    SELECT id FROM dividend_events 
                    WHERE company_name = ? AND ticker = ? AND ex_dividend_date = ?
                ''', (dividend['company_name'], dividend['ticker'], dividend['ex_dividend_date']))
                
                if cursor.fetchone():
                    # 업데이트
                    cursor.execute('''
                        UPDATE dividend_events SET
                            payment_date = ?, dividend_amount = ?, dividend_yield = ?,
                            currency = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE company_name = ? AND ticker = ? AND ex_dividend_date = ?
                    ''', (
                        dividend['payment_date'], dividend['dividend_amount'], 
                        dividend['dividend_yield'], dividend['currency'],
                        dividend['company_name'], dividend['ticker'], dividend['ex_dividend_date']
                    ))
                else:
                    # 삽입
                    cursor.execute('''
                        INSERT INTO dividend_events 
                        (company_name, ticker, country, ex_dividend_date, payment_date, 
                         dividend_amount, dividend_yield, currency)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        dividend['company_name'], dividend['ticker'], dividend['country'],
                        dividend['ex_dividend_date'], dividend['payment_date'],
                        dividend['dividend_amount'], dividend['dividend_yield'], dividend['currency']
                    ))
            
            conn.commit()
            logger.info(f"{len(dividends)}개의 배당 데이터를 저장했습니다.")
            
        except Exception as e:
            logger.error(f"데이터 저장 오류: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_dividend_calendar_for_month(self, year: int, month: int) -> Dict:
        """특정 월의 배당 캘린더 데이터 반환 (달력용)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # 해당 월의 데이터 조회
            start_date = f"{year}-{month:02d}-01"
            if month == 12:
                end_date = f"{year + 1}-01-01"
            else:
                end_date = f"{year}-{month + 1:02d}-01"
            
            cursor.execute('''
                SELECT company_name, ticker, country, ex_dividend_date, payment_date,
                       dividend_amount, dividend_yield, currency
                FROM dividend_events
                WHERE ex_dividend_date >= ? AND ex_dividend_date < ?
                ORDER BY ex_dividend_date, company_name
            ''', (start_date, end_date))
            
            rows = cursor.fetchall()
            
            # 날짜별로 그룹화
            calendar_data = {}
            for row in rows:
                company_name, ticker, country, ex_date, pay_date, amount, yield_val, currency = row
                
                if ex_date:
                    day = int(ex_date.split('-')[2])
                    
                    if day not in calendar_data:
                        calendar_data[day] = {'kospi': [], 'kosdaq': []}
                    
                    dividend_info = {
                        'name': company_name,
                        'ticker': ticker,
                        'ex_date': ex_date.replace('-', ''),
                        'pay_date': pay_date.replace('-', '') if pay_date else '',
                        'per_share': amount,
                        'yield': yield_val,
                        'currency': currency,
                        'country': country,
                        'market': self._get_market_type(ticker, country)
                    }
                    
                    # 시장별 구분
                    if country == 'KR':
                        calendar_data[day]['kospi'].append(dividend_info)
                    else:
                        # 해외 종목을 시장별로 구분
                        market_key = f'overseas_{dividend_info["market"].lower()}'
                        if market_key not in calendar_data[day]:
                            calendar_data[day][market_key] = []
                        calendar_data[day][market_key].append(dividend_info)
            
            return calendar_data
            
        except Exception as e:
            logger.error(f"캘린더 데이터 조회 오류: {e}")
            return {}
        finally:
            conn.close()
    
    def update_dividend_data(self, months_ahead: int = 3):
        """배당 데이터 자동 업데이트"""
        try:
            current_date = datetime.now()
            end_date = current_date + timedelta(days=months_ahead * 30)
            
            start_date_str = current_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"배당 데이터 업데이트 시작: {start_date_str} ~ {end_date_str}")
            
            # 데이터 수집
            dividends = self.get_dividend_calendar_data(start_date_str, end_date_str)
            
            if dividends:
                # 데이터베이스에 저장
                self.save_dividend_data(dividends)
                logger.info(f"배당 데이터 업데이트 완료: {len(dividends)}개 항목")
            else:
                logger.warning("수집된 배당 데이터가 없습니다.")
                
        except Exception as e:
            logger.error(f"배당 데이터 업데이트 오류: {e}")

# 사용 예시
if __name__ == "__main__":
    crawler = InvestingDividendCrawler()
    
    # 최근 3개월 데이터 업데이트
    crawler.update_dividend_data(months_ahead=3)
    
    # 2025년 9월 데이터 조회
    calendar_data = crawler.get_dividend_calendar_for_month(2025, 9)
    print(json.dumps(calendar_data, indent=2, ensure_ascii=False))
