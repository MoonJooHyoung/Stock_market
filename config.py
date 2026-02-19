"""
DART API 설정 파일
"""

import os
from typing import Optional


class Config:
    """DART API 설정 클래스"""
    
    # API 기본 설정
    API_BASE_URL = "https://opendart.fss.or.kr/api"
    API_KEY = os.getenv('DART_API_KEY', '')
    
    # 요청 제한 설정
    REQUEST_DELAY = 0.1  # API 요청 간 딜레이 (초)
    MAX_RETRIES = 3  # 최대 재시도 횟수
    TIMEOUT = 30  # 요청 타임아웃 (초)
    
    # 보고서 코드 매핑
    REPORT_CODES = {
        'annual': '11011',      # 사업보고서
        'semi_annual': '11012', # 반기보고서
        'q1': '11013',          # 1분기보고서
        'q3': '11014'           # 3분기보고서
    }
    
    # 출력 형식
    OUTPUT_FORMATS = ['json', 'xml']
    
    @classmethod
    def get_api_key(cls) -> str:
        """API 키를 반환합니다."""
        if not cls.API_KEY:
            raise ValueError(
                "DART API 키가 설정되지 않았습니다. "
                "환경변수 DART_API_KEY를 설정하거나 config.py에서 직접 설정하세요."
            )
        return cls.API_KEY
    
    @classmethod
    def validate_config(cls) -> bool:
        """설정이 유효한지 검증합니다."""
        try:
            cls.get_api_key()
            return True
        except ValueError:
            return False


# 환경별 설정
class DevelopmentConfig(Config):
    """개발 환경 설정"""
    REQUEST_DELAY = 0.2  # 개발시 더 긴 딜레이
    DEBUG = True


class ProductionConfig(Config):
    """운영 환경 설정"""
    REQUEST_DELAY = 0.1
    DEBUG = False


class TestConfig(Config):
    """테스트 환경 설정"""
    REQUEST_DELAY = 0.0  # 테스트시 딜레이 없음
    DEBUG = True


# 현재 환경에 따른 설정 선택
def get_config() -> Config:
    """현재 환경에 맞는 설정을 반환합니다."""
    env = os.getenv('ENVIRONMENT', 'development').lower()
    
    if env == 'production':
        return ProductionConfig()
    elif env == 'test':
        return TestConfig()
    else:
        return DevelopmentConfig()

