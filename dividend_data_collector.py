"""
배당 데이터 수집 및 달력 표시를 위한 메인 시스템
DART API와 공공데이터포털 API를 사용하여 배당 정보를 수집하고 달력에 표시합니다.
"""

import os
import sqlite3
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
from dart_dividend_api import DartDividendAPI

class DividendDataCollector:
    """배당 데이터 수집 및 관리 클래스"""
    
    def __init__(self, dart_api_key: str, holiday_api_key: str):
        """
        초기화
        
        Args:
            dart_api_key (str): DART API 키
            holiday_api_key (str): 공공데이터포털 API 키
        """
        self.dart_api = DartDividendAPI(dart_api_key)
        self.holiday_api_key = holiday_api_key
        self.holiday_api_url = "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
        
        # 데이터베이스 초기화
        self.init_database()
        
    def init_database(self):
        """데이터베이스 테이블 초기화"""
        conn = sqlite3.connect('dividend_calendar.db')
        cursor = conn.cursor()
        
        # 배당 정보 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dividends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corp_code TEXT NOT NULL,
                corp_name TEXT NOT NULL,
                market TEXT NOT NULL,
                ex_dividend_date TEXT,
                payment_date TEXT,
                dividend_per_share REAL,
                total_dividend REAL,
                announcement_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 공휴일 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS holidays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                holiday_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(year, month, day)
            )
        ''')
        
        # 종목 정보 테이블
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                corp_code TEXT PRIMARY KEY,
                corp_name TEXT NOT NULL,
                market TEXT NOT NULL,
                stock_code TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def collect_holiday_data(self, year: int) -> Dict[int, List[int]]:
        """공휴일 데이터 수집"""
        holidays = {}
        
        for month in range(1, 13):
            try:
                params = {
                    'solYear': year,
                    'solMonth': month,
                    'ServiceKey': self.holiday_api_key,
                    '_type': 'json',
                    'numOfRows': 50
                }
                
                response = requests.get(self.holiday_api_url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('response', {}).get('body', {}).get('items'):
                    items = data['response']['body']['items']['item']
                    if not isinstance(items, list):
                        items = [items]
                    
                    month_holidays = []
                    for item in items:
                        if item.get('isHoliday') == 'Y':
                            day = int(str(item['locdate'])[-2:])
                            month_holidays.append(day)
                            
                            # 데이터베이스에 저장
                            self.save_holiday_to_db(year, month, day, item.get('dateName', ''))
                    
                    if month_holidays:
                        holidays[month] = month_holidays
                
                time.sleep(0.1)  # API 호출 제한
                
            except Exception as e:
                print(f"공휴일 데이터 수집 실패 ({year}년 {month}월): {e}")
                
        return holidays
    
    def save_holiday_to_db(self, year: int, month: int, day: int, holiday_name: str):
        """공휴일 데이터를 데이터베이스에 저장"""
        conn = sqlite3.connect('dividend_calendar.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO holidays (year, month, day, holiday_name)
            VALUES (?, ?, ?, ?)
        ''', (year, month, day, holiday_name))
        
        conn.commit()
        conn.close()
    
    def collect_dividend_data(self, year: int) -> List[Dict]:
        """배당 데이터 수집"""
        dividends = []
        
        try:
            # 1. 모든 상장회사 목록 가져오기
            companies = self.dart_api.get_company_list()
            print(f"총 {len(companies)}개 회사 정보를 가져왔습니다.")
            
            # 2. 각 회사별로 배당 정보 조회
            for i, company in enumerate(companies[:100]):  # 테스트를 위해 100개만
                try:
                    corp_code = company['corp_code']
                    corp_name = company['corp_name']
                    market = company.get('market', 'KOSPI')
                    
                    # 배당 정보 조회 (사업보고서 기준)
                    dividend_info = self.dart_api.get_dividend_info(corp_code, year, '11011')
                    
                    if dividend_info:
                        for dividend in dividend_info:
                            dividend['corp_code'] = corp_code
                            dividend['corp_name'] = corp_name
                            dividend['market'] = market
                            dividends.append(dividend)
                            
                            # 데이터베이스에 저장
                            self.save_dividend_to_db(dividend)
                    
                    print(f"진행률: {i+1}/{min(100, len(companies))} - {corp_name}")
                    time.sleep(0.2)  # API 호출 제한
                    
                except Exception as e:
                    print(f"회사 {company.get('corp_name', 'Unknown')} 배당 정보 수집 실패: {e}")
                    continue
                    
        except Exception as e:
            print(f"배당 데이터 수집 실패: {e}")
            
        return dividends
    
    def save_dividend_to_db(self, dividend: Dict):
        """배당 데이터를 데이터베이스에 저장"""
        conn = sqlite3.connect('dividend_calendar.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO dividends 
            (corp_code, corp_name, market, ex_dividend_date, payment_date, 
             dividend_per_share, total_dividend, announcement_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dividend.get('corp_code'),
            dividend.get('corp_name'),
            dividend.get('market'),
            dividend.get('ex_dividend_date'),
            dividend.get('payment_date'),
            dividend.get('dividend_per_share'),
            dividend.get('total_dividend'),
            dividend.get('announcement_date')
        ))
        
        conn.commit()
        conn.close()
    
    def get_dividend_calendar_data(self, year: int, month: int) -> Dict:
        """달력 표시용 배당 데이터 조회"""
        conn = sqlite3.connect('dividend_calendar.db')
        cursor = conn.cursor()
        
        # 해당 월의 배당 데이터 조회 (배당락일만)
        start_date = f"{year:04d}{month:02d}01"
        if month == 12:
            end_date = f"{year+1:04d}0101"
        else:
            end_date = f"{year:04d}{month+1:02d}01"
        
        cursor.execute('''
            SELECT corp_name, market, ex_dividend_date, payment_date, 
                   dividend_per_share, total_dividend
            FROM dividends
            WHERE ex_dividend_date >= ? AND ex_dividend_date < ?
            ORDER BY ex_dividend_date
        ''', (start_date, end_date))
        
        dividends = cursor.fetchall()
        
        # 날짜별로 그룹화 (중복 제거)
        calendar_data = {}
        processed_stocks = set()  # 중복 제거용
        
        for dividend in dividends:
            corp_name, market, ex_date, pay_date, per_share, total = dividend
            
            # 중복 제거: 같은 종목의 같은 날짜는 한 번만 표시
            stock_key = f"{corp_name}_{ex_date}"
            if stock_key in processed_stocks:
                continue
            processed_stocks.add(stock_key)
            
            # 배당락일 처리
            if ex_date:
                ex_date_obj = datetime.strptime(ex_date, '%Y%m%d')
                day = ex_date_obj.day
                if day not in calendar_data:
                    calendar_data[day] = {'kospi': [], 'kosdaq': []}
                
                calendar_data[day][market.lower()].append({
                    'name': corp_name,
                    'ex_date': ex_date,
                    'pay_date': pay_date,
                    'per_share': per_share,
                    'total': total
                })
        
        conn.close()
        return calendar_data
    
    def get_holiday_data(self, year: int) -> Dict[int, List[int]]:
        """공휴일 데이터 조회"""
        conn = sqlite3.connect('dividend_calendar.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT month, day FROM holidays WHERE year = ?
        ''', (year,))
        
        holidays = {}
        for month, day in cursor.fetchall():
            if month not in holidays:
                holidays[month] = []
            holidays[month].append(day)
        
        conn.close()
        return holidays

def main():
    """메인 실행 함수"""
    # API 키 설정
    dart_api_key = os.getenv('DART_API_KEY')
    holiday_api_key = os.getenv('HOLIDAY_API_KEY')
    
    if not dart_api_key or not holiday_api_key:
        print("API 키가 설정되지 않았습니다.")
        print("환경변수 DART_API_KEY와 HOLIDAY_API_KEY를 설정해주세요.")
        return
    
    # 데이터 수집기 초기화
    collector = DividendDataCollector(dart_api_key, holiday_api_key)
    
    # 2025년 데이터 수집
    year = 2025
    print(f"{year}년 데이터 수집을 시작합니다...")
    
    # 공휴일 데이터 수집
    print("공휴일 데이터 수집 중...")
    holidays = collector.collect_holiday_data(year)
    print(f"공휴일 데이터 수집 완료: {holidays}")
    
    # 배당 데이터 수집
    print("배당 데이터 수집 중...")
    dividends = collector.collect_dividend_data(year)
    print(f"배당 데이터 수집 완료: {len(dividends)}건")
    
    print("데이터 수집이 완료되었습니다!")

if __name__ == "__main__":
    main()
