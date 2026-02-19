"""
배당 데이터 자동 업데이트 스케줄러
정기적으로 Investing.com에서 배당 데이터를 수집합니다.
"""

import schedule
import time
import logging
from datetime import datetime
from investing_crawler import InvestingDividendCrawler
import threading

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DividendScheduler:
    """배당 데이터 자동 업데이트 스케줄러"""
    
    def __init__(self, db_path: str = "dividend_calendar.db"):
        self.crawler = InvestingDividendCrawler(db_path)
        self.running = False
        self.thread = None
    
    def update_dividend_data(self):
        """배당 데이터 업데이트 작업"""
        try:
            logger.info("배당 데이터 자동 업데이트 시작")
            
            # 최근 3개월 데이터 업데이트
            self.crawler.update_dividend_data(months_ahead=3)
            
            logger.info("배당 데이터 자동 업데이트 완료")
            
        except Exception as e:
            logger.error(f"배당 데이터 업데이트 중 오류 발생: {e}")
    
    def start_scheduler(self):
        """스케줄러 시작"""
        if self.running:
            logger.warning("스케줄러가 이미 실행 중입니다.")
            return
        
        self.running = True
        
        # 스케줄 설정
        # 매일 오전 9시에 업데이트
        schedule.every().day.at("09:00").do(self.update_dividend_data)
        
        # 매주 월요일 오전 10시에 전체 데이터 재수집
        schedule.every().monday.at("10:00").do(self._full_update)
        
        # 매시간 상태 체크
        schedule.every().hour.do(self._status_check)
        
        logger.info("배당 데이터 스케줄러가 시작되었습니다.")
        logger.info("스케줄:")
        logger.info("- 매일 09:00: 배당 데이터 업데이트")
        logger.info("- 매주 월요일 10:00: 전체 데이터 재수집")
        logger.info("- 매시간: 상태 체크")
        
        # 스케줄러 실행
        self.thread = threading.Thread(target=self._run_scheduler)
        self.thread.daemon = True
        self.thread.start()
    
    def stop_scheduler(self):
        """스케줄러 중지"""
        self.running = False
        schedule.clear()
        logger.info("배당 데이터 스케줄러가 중지되었습니다.")
    
    def _run_scheduler(self):
        """스케줄러 실행 루프"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 체크
            except Exception as e:
                logger.error(f"스케줄러 실행 중 오류: {e}")
                time.sleep(60)
    
    def _full_update(self):
        """전체 데이터 재수집"""
        try:
            logger.info("전체 배당 데이터 재수집 시작")
            
            # 최근 6개월 데이터 재수집
            self.crawler.update_dividend_data(months_ahead=6)
            
            logger.info("전체 배당 데이터 재수집 완료")
            
        except Exception as e:
            logger.error(f"전체 데이터 재수집 중 오류: {e}")
    
    def _status_check(self):
        """상태 체크"""
        try:
            # 데이터베이스 상태 확인
            import sqlite3
            conn = sqlite3.connect(self.crawler.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*) as total_count,
                       MAX(created_at) as last_update
                FROM dividend_events
            ''')
            
            result = cursor.fetchone()
            conn.close()
            
            logger.info(f"데이터베이스 상태 - 총 배당 이벤트: {result[0]}개, 마지막 업데이트: {result[1]}")
            
        except Exception as e:
            logger.error(f"상태 체크 중 오류: {e}")
    
    def run_once(self):
        """한 번만 실행"""
        logger.info("수동 배당 데이터 업데이트 실행")
        self.update_dividend_data()

def main():
    """메인 함수"""
    scheduler = DividendScheduler()
    
    try:
        # 스케줄러 시작
        scheduler.start_scheduler()
        
        # 프로그램이 종료될 때까지 대기
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        scheduler.stop_scheduler()
    except Exception as e:
        logger.error(f"스케줄러 실행 중 오류: {e}")
        scheduler.stop_scheduler()

if __name__ == "__main__":
    main()
