"""
DART API 유틸리티 함수들
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json


def format_dividend_data(dividend_list: List[Dict]) -> pd.DataFrame:
    """
    배당 데이터를 pandas DataFrame으로 변환합니다.
    
    Args:
        dividend_list (List[Dict]): 배당 정보 리스트
        
    Returns:
        pd.DataFrame: 포맷된 배당 데이터
    """
    if not dividend_list:
        return pd.DataFrame()
    
    # 데이터 정리
    formatted_data = []
    for dividend in dividend_list:
        formatted_data.append({
            '접수번호': dividend.get('rcept_no', ''),
            '법인명': dividend.get('corp_name', ''),
            '구분': dividend.get('se', ''),
            '주식종류': dividend.get('stock_knd', ''),
            '당기': _parse_number(dividend.get('thstrm', '0')),
            '전기': _parse_number(dividend.get('frmtrm', '0')),
            '전전기': _parse_number(dividend.get('lwfr', '0')),
            '결산기준일': dividend.get('stlm_dt', '')
        })
    
    return pd.DataFrame(formatted_data)


def _parse_number(value: str) -> float:
    """문자열 숫자를 float로 변환합니다."""
    if not value or value == '':
        return 0.0
    
    # 쉼표 제거 후 변환
    try:
        return float(value.replace(',', ''))
    except (ValueError, AttributeError):
        return 0.0


def calculate_dividend_yield(dividend_amount: float, stock_price: float) -> float:
    """
    배당 수익률을 계산합니다.
    
    Args:
        dividend_amount (float): 배당금
        stock_price (float): 주가
        
    Returns:
        float: 배당 수익률 (%)
    """
    if stock_price <= 0:
        return 0.0
    
    return (dividend_amount / stock_price) * 100


def analyze_dividend_trend(dividend_data: pd.DataFrame) -> Dict:
    """
    배당 트렌드를 분석합니다.
    
    Args:
        dividend_data (pd.DataFrame): 배당 데이터
        
    Returns:
        Dict: 분석 결과
    """
    if dividend_data.empty:
        return {'error': '분석할 데이터가 없습니다.'}
    
    analysis = {}
    
    # 당기 배당금 분석
    current_dividends = dividend_data['당기'].dropna()
    if not current_dividends.empty:
        analysis['당기_평균'] = current_dividends.mean()
        analysis['당기_최대'] = current_dividends.max()
        analysis['당기_최소'] = current_dividends.min()
        analysis['당기_총합'] = current_dividends.sum()
    
    # 전기 대비 증감률 계산
    if '전기' in dividend_data.columns:
        prev_dividends = dividend_data['전기'].dropna()
        if not prev_dividends.empty and not current_dividends.empty:
            growth_rate = ((current_dividends.mean() - prev_dividends.mean()) / prev_dividends.mean()) * 100
            analysis['전기대비_증감률'] = growth_rate
    
    # 배당 구분별 분석
    if '구분' in dividend_data.columns:
        dividend_by_type = dividend_data.groupby('구분')['당기'].sum()
        analysis['구분별_배당'] = dividend_by_type.to_dict()
    
    return analysis


def export_to_excel(dividend_data: pd.DataFrame, filename: str) -> bool:
    """
    배당 데이터를 Excel 파일로 내보냅니다.
    
    Args:
        dividend_data (pd.DataFrame): 배당 데이터
        filename (str): 파일명
        
    Returns:
        bool: 성공 여부
    """
    try:
        dividend_data.to_excel(filename, index=False, engine='openpyxl')
        return True
    except Exception as e:
        print(f"Excel 내보내기 실패: {e}")
        return False


def export_to_csv(dividend_data: pd.DataFrame, filename: str) -> bool:
    """
    배당 데이터를 CSV 파일로 내보냅니다.
    
    Args:
        dividend_data (pd.DataFrame): 배당 데이터
        filename (str): 파일명
        
    Returns:
        bool: 성공 여부
    """
    try:
        dividend_data.to_csv(filename, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        print(f"CSV 내보내기 실패: {e}")
        return False


def create_dividend_report(api, corp_code: str, years: List[int]) -> Dict:
    """
    배당 리포트를 생성합니다.
    
    Args:
        api: DartDividendAPI 인스턴스
        corp_code (str): 고유번호
        years (List[int]): 조회할 연도 리스트
        
    Returns:
        Dict: 배당 리포트
    """
    report = {
        'corp_code': corp_code,
        'generated_at': datetime.now().isoformat(),
        'years': years,
        'data': {}
    }
    
    for year in years:
        try:
            summary = api.get_dividend_summary(corp_code, year)
            report['data'][str(year)] = summary
            
            # DataFrame으로 변환하여 분석
            if 'dividends' in summary and summary['dividends']:
                df = format_dividend_data(summary['dividends'])
                analysis = analyze_dividend_trend(df)
                report['data'][str(year)]['analysis'] = analysis
                
        except Exception as e:
            report['data'][str(year)] = {'error': str(e)}
    
    return report


def save_report_to_file(report: Dict, filename: str) -> bool:
    """
    리포트를 JSON 파일로 저장합니다.
    
    Args:
        report (Dict): 리포트 데이터
        filename (str): 파일명
        
    Returns:
        bool: 성공 여부
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"리포트 저장 실패: {e}")
        return False


def load_report_from_file(filename: str) -> Optional[Dict]:
    """
    JSON 파일에서 리포트를 로드합니다.
    
    Args:
        filename (str): 파일명
        
    Returns:
        Optional[Dict]: 리포트 데이터
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"리포트 로드 실패: {e}")
        return None


def get_recent_years(count: int = 5) -> List[int]:
    """
    최근 N년의 연도 리스트를 반환합니다.
    
    Args:
        count (int): 조회할 연도 수
        
    Returns:
        List[int]: 연도 리스트
    """
    current_year = datetime.now().year
    return list(range(current_year - count + 1, current_year + 1))


def validate_corp_code(corp_code: str) -> bool:
    """
    고유번호 형식을 검증합니다.
    
    Args:
        corp_code (str): 고유번호
        
    Returns:
        bool: 유효성 여부
    """
    if not corp_code:
        return False
    
    # 8자리 숫자인지 확인
    if len(corp_code) != 8:
        return False
    
    try:
        int(corp_code)
        return True
    except ValueError:
        return False


def format_currency(amount: float) -> str:
    """
    금액을 한국 원화 형식으로 포맷합니다.
    
    Args:
        amount (float): 금액
        
    Returns:
        str: 포맷된 금액 문자열
    """
    if amount == 0:
        return "0원"
    
    return f"{amount:,.0f}원"


def get_report_type_name(report_code: str) -> str:
    """
    보고서 코드를 한글명으로 변환합니다.
    
    Args:
        report_code (str): 보고서 코드
        
    Returns:
        str: 보고서 한글명
    """
    report_types = {
        '11011': '사업보고서',
        '11012': '반기보고서',
        '11013': '1분기보고서',
        '11014': '3분기보고서'
    }
    
    return report_types.get(report_code, '알 수 없는 보고서')
