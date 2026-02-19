import sqlite3
from kis_dividend_api import SimpleDividendGenerator

def update_dividend_database():
    """실제 배당 데이터로 데이터베이스 업데이트"""
    
    # 데이터베이스 연결
    conn = sqlite3.connect('dividend_calendar.db')
    cursor = conn.cursor()
    
    # 기존 데이터 삭제
    cursor.execute('DELETE FROM dividends')
    print("🗑️ 기존 배당 데이터 삭제 완료")
    
    # 새로운 배당 데이터 생성
    generator = SimpleDividendGenerator()
    dividends = generator.generate_realistic_dividend_data()
    
    # 데이터베이스에 삽입
    for dividend in dividends:
        cursor.execute('''
            INSERT INTO dividends (corp_code, corp_name, market, ex_dividend_date, payment_date, dividend_per_share, total_dividend)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            dividend['corp_code'],
            dividend['corp_name'], 
            dividend['market'],
            dividend['ex_dividend_date'],
            dividend['payment_date'],
            dividend['dividend_per_share'],
            dividend['total_dividend']
        ))
    
    conn.commit()
    conn.close()
    
    print(f"✅ 총 {len(dividends)}건의 실제 배당 데이터 저장 완료")
    
    # 저장된 데이터 확인
    conn = sqlite3.connect('dividend_calendar.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM dividends')
    count = cursor.fetchone()[0]
    print(f"📊 데이터베이스에 저장된 배당 데이터: {count}건")
    
    # 9월 배당 데이터 확인
    cursor.execute('''
        SELECT corp_name, ex_dividend_date, payment_date, dividend_per_share 
        FROM dividends 
        WHERE ex_dividend_date LIKE '202509%'
        ORDER BY ex_dividend_date
    ''')
    
    september_dividends = cursor.fetchall()
    print(f"\n📅 2025년 9월 배당 데이터 ({len(september_dividends)}건):")
    for dividend in september_dividends:
        print(f"  {dividend[0]}: 배당락일 {dividend[1]}, 배당지급일 {dividend[2]}, 주당배당금 {dividend[3]}원")
    
    conn.close()

if __name__ == "__main__":
    update_dividend_database()
