"""
배당 달력 웹 서버
Flask를 사용하여 배당 데이터를 제공하는 웹 서버입니다.
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import sqlite3
import json
from datetime import datetime
from dividend_data_collector import DividendDataCollector
from real_dividend_crawler import RealDividendCrawler
from scheduler import DividendScheduler
import os
import threading
import time

app = Flask(__name__)
CORS(app)  # CORS 허용

# 전역 데이터 수집기
collector = None
crawler = None
scheduler = None

def init_collector():
    """데이터 수집기 초기화"""
    global collector, crawler, scheduler
    
    # 기존 DART API 수집기
    dart_api_key = os.getenv('DART_API_KEY')
    holiday_api_key = os.getenv('HOLIDAY_API_KEY')
    
    if dart_api_key and holiday_api_key:
        collector = DividendDataCollector(dart_api_key, holiday_api_key)
    else:
        print("DART API 키가 설정되지 않았습니다.")
    
    # 실제 배당 데이터 크롤러 초기화
    crawler = RealDividendCrawler()
    print("실제 배당 데이터 크롤러가 초기화되었습니다.")
    
    # 스케줄러 초기화 (자동 업데이트 비활성화)
    # scheduler = DividendScheduler()
    # scheduler.start_scheduler()
    # print("자동 업데이트 스케줄러가 시작되었습니다.")

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('calendar_app.html')

@app.route('/favicon.ico')
def favicon():
    """파비콘 요청 처리"""
    return '', 204  # No Content

@app.route('/api/calendar/<int:year>/<int:month>')
def get_calendar_data(year, month):
    """달력 데이터 API - 실제 크롤링 데이터 사용"""
    try:
        # 크롤링된 배당 데이터 조회
        dividend_data = {}
        if crawler:
            dividend_data = crawler.get_dividend_calendar_for_month(year, month)
        
        # 기존 DART API 데이터가 있으면 병합
        if collector:
            dart_data = collector.get_dividend_calendar_data(year, month)
            # DART 데이터와 크롤링 데이터 병합
            for day, events in dart_data.items():
                if day not in dividend_data:
                    dividend_data[day] = {'kospi': [], 'kosdaq': []}
                if 'kospi' in events:
                    dividend_data[day]['kospi'].extend(events['kospi'])
                if 'kosdaq' in events:
                    dividend_data[day]['kosdaq'].extend(events['kosdaq'])
        
        # 공휴일 데이터 조회
        holiday_data = collector.get_holiday_data(year) if collector else {}
        
        return jsonify({
            'success': True,
            'dividends': dividend_data,
            'holidays': holiday_data.get(month, [])
        })
    except Exception as e:
        print(f"달력 데이터 API 오류: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/holidays/<int:year>')
def get_holidays(year):
    """공휴일 데이터 API"""
    try:
        holiday_data = collector.get_holiday_data(year) if collector else {}
        return jsonify({
            'success': True,
            'holidays': holiday_data
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/collect/<int:year>')
def collect_data(year):
    """데이터 수집 API"""
    try:
        if not collector:
            return jsonify({
                'success': False,
                'error': '데이터 수집기가 초기화되지 않았습니다.'
            }), 500
        
        # 공휴일 데이터 수집
        holidays = collector.collect_holiday_data(year)
        
        # 배당 데이터 수집 (백그라운드에서 실행)
        import threading
        def collect_dividends():
            collector.collect_dividend_data(year)
        
        thread = threading.Thread(target=collect_dividends)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'{year}년 데이터 수집을 시작했습니다.',
            'holidays': holidays
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/status')
def get_status():
    """서버 상태 확인"""
    return jsonify({
        'success': True,
        'status': 'running',
        'collector_initialized': collector is not None,
        'crawler_initialized': crawler is not None
    })

@app.route('/api/crawl/update', methods=['POST'])
def update_dividend_data():
    """배당 데이터 수동 업데이트"""
    try:
        if not crawler:
            return jsonify({
                'success': False,
                'error': '크롤러가 초기화되지 않았습니다.'
            }), 500
        
        # 백그라운드에서 데이터 업데이트
        def update_data():
            crawler.update_dividend_data(months_ahead=3)
        
        thread = threading.Thread(target=update_data)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': '배당 데이터 업데이트를 시작했습니다.'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/crawl/status')
def get_crawl_status():
    """크롤링 상태 확인"""
    try:
        if not crawler:
            return jsonify({
                'success': False,
                'error': '크롤러가 초기화되지 않았습니다.'
            }), 500
        
        # 데이터베이스에서 최근 데이터 확인
        conn = sqlite3.connect(crawler.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) as total_count,
                   MAX(created_at) as last_update,
                   COUNT(DISTINCT DATE(ex_dividend_date)) as unique_dates
            FROM dividend_events
        ''')
        
        result = cursor.fetchone()
        conn.close()
        
        return jsonify({
            'success': True,
            'total_dividends': result[0],
            'last_update': result[1],
            'unique_dates': result[2]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/crawl/companies')
def get_companies():
    """수집된 기업 목록 조회"""
    try:
        if not crawler:
            return jsonify({
                'success': False,
                'error': '크롤러가 초기화되지 않았습니다.'
            }), 500
        
        conn = sqlite3.connect(crawler.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT company_name, ticker, country, COUNT(*) as dividend_count
            FROM dividend_events
            GROUP BY company_name, ticker, country
            ORDER BY dividend_count DESC, company_name
        ''')
        
        companies = []
        for row in cursor.fetchall():
            companies.append({
                'name': row[0],
                'ticker': row[1],
                'country': row[2],
                'dividend_count': row[3]
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'companies': companies
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/scheduler/start', methods=['POST'])
def start_scheduler():
    """스케줄러 시작"""
    try:
        global scheduler
        if not scheduler:
            scheduler = DividendScheduler()
        
        scheduler.start_scheduler()
        
        return jsonify({
            'success': True,
            'message': '자동 업데이트 스케줄러가 시작되었습니다.'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/scheduler/stop', methods=['POST'])
def stop_scheduler():
    """스케줄러 중지"""
    try:
        global scheduler
        if scheduler:
            scheduler.stop_scheduler()
            return jsonify({
                'success': True,
                'message': '자동 업데이트 스케줄러가 중지되었습니다.'
            })
        else:
            return jsonify({
                'success': False,
                'error': '스케줄러가 실행되지 않았습니다.'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/scheduler/status')
def get_scheduler_status():
    """스케줄러 상태 확인"""
    try:
        global scheduler
        if scheduler and scheduler.running:
            return jsonify({
                'success': True,
                'running': True,
                'message': '스케줄러가 실행 중입니다.'
            })
        else:
            return jsonify({
                'success': True,
                'running': False,
                'message': '스케줄러가 중지되었습니다.'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    init_collector()
    app.run(debug=True, host='0.0.0.0', port=5000)
