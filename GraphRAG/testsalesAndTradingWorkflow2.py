//cypher query

// Clear existing data (optional, for a fresh start)
MATCH (n) DETACH DELETE n;

// 2. Create sample Nodes and Relationships to implicitly define the schema

// Institutional Clients
MERGE (client1:InstitutionalClient {id: 'IC001', name: 'Global Asset Management', sector: 'Financial'})
MERGE (client2:InstitutionalClient {id: 'IC002', name: 'Tech Innovations Fund', sector: 'Technology'})

// Traders
MERGE (trader1:Trader {id: 'TR001', name: 'Alice Smith', email: 'alice@example.com'})
MERGE (trader2:Trader {id: 'TR002', name: 'Bob Johnson', email: 'bob@example.com'})

// Stocks
MERGE (stock1:Stock {symbol: 'AAPL', name: 'Apple Inc.', sector: 'Technology'})
MERGE (stock2:Stock {symbol: 'MSFT', name: 'Microsoft Corp.', sector: 'Technology'})
MERGE (stock3:Stock {symbol: 'GOOG', name: 'Alphabet Inc.', sector: 'Technology'})
MERGE (stock4:Stock {symbol: 'JPM', name: 'JPMorgan Chase & Co.', sector: 'Finance'})

// Order Types
MERGE (type1:OrderType {type: 'Buy', description: 'Purchase stock'})
MERGE (type2:OrderType {type: 'Sell', description: 'Sell stock'})

// Markets
MERGE (market1:Market {name: 'NASDAQ', location: 'USA'})
MERGE (market2:Market {name: 'NYSE', location: 'USA'})

// Compliance Rules
MERGE (rule1:ComplianceRule {rule_id: 'CR001', name: 'Max Order Size', threshold: 100000, type: 'TradeLimit'})
MERGE (rule2:ComplianceRule {rule_id: 'CR002', name: 'Restricted Stock List', type: 'Blacklist'})
MERGE (rule3:ComplianceRule {rule_id: 'CR003', name: 'Risk Profile Match', type: 'RiskCheck'})

// Risk Profiles
MERGE (profile1:RiskProfile {profile_id: 'RP001', level: 'Conservative', description: 'Low risk tolerance'})
MERGE (profile2:RiskProfile {profile_id: 'RP002', level: 'Moderate', description: 'Medium risk tolerance'})
MERGE (profile3:RiskProfile {profile_id: 'RP003', level: 'Aggressive', description: 'High risk tolerance'})

// Relationships
MERGE (client1)-[:HAS_TRADER]->(trader1)
MERGE (client2)-[:HAS_TRADER]->(trader2)

// Client Risk Profile
MERGE (client1)-[:HAS_PROFILE]->(profile1)
MERGE (client2)-[:HAS_PROFILE]->(profile2)

// Stocks Traded On Markets
MERGE (stock1)-[:TRADED_ON]->(market1)
MERGE (stock2)-[:TRADED_ON]->(market1)
MERGE (stock3)-[:TRADED_ON]->(market1)
MERGE (stock4)-[:TRADED_ON]->(market2)

// Orders (sample)
MERGE (order1:Order {order_id: 'ORD001', quantity: 500, price: 150.0, status: 'New', timestamp: '2023-10-26T10:00:00'})
MERGE (trader1)-[:PLACES]->(order1)
MERGE (order1)-[:FOR_STOCK]->(stock1)
MERGE (order1)-[:HAS_TYPE]->(type1)
MERGE (order1)-[:CHECKED_AGAINST]->(rule1)
MERGE (order1)-[:CHECKED_AGAINST]->(rule3)

MERGE (order2:Order {order_id: 'ORD002', quantity: 1000, price: 160.0, status: 'Executed', timestamp: '2023-10-26T10:05:00'})
MERGE (trader2)-[:PLACES]->(order2)
MERGE (order2)-[:FOR_STOCK]->(stock2)
MERGE (order2)-[:HAS_TYPE]->(type2)
MERGE (order2)-[:CHECKED_AGAINST]->(rule1)
MERGE (order2)-[:CHECKED_AGAINST]->(rule3)

RETURN 'Stock order ontology schema and sample data loaded.' AS Result;



# 가상환경(선택)
# python -m venv venv && source venv/bin/activate

# 필요 패키지
!pip install neo4j pandas tqdm

URI="neo4j+s://2815798c.databases.neo4j.io"
USER="neo4j"
PASSWORD="M-5hZ5h6Q197RrlzPBI4LaEoiblCblaXfM1ZtsfuRhE"


# --------------------------------------------------------------
# trading_workflow.py
# --------------------------------------------------------------
import os
from neo4j import GraphDatabase
import pandas as pd
from tqdm import tqdm   # 진행 표시용 (선택)

# ------------------- Neo4j 연결 ------------------------------
# URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
# USER     = os.getenv("NEO4J_USER", "neo4j")
# PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


# ------------------- 유틸리티 -------------------------------
def run_query(tx, query: str, params: dict = None):
    """Transaction‑level 간단 래퍼."""
    result = tx.run(query, params or {})
    return [record.data() for record in result]


# ------------------- ① 클라이언트‑별 주문 현황 -----------------
def client_orders(client_id: str) -> pd.DataFrame:
    """
    지정된 InstitutionalClient(ID) 가 보유한 모든 주문을 DataFrame 으로 반환.
    컬럼: order_id, trader, stock, qty, price, total_value, status, timestamp
    """
    cypher = """
    MATCH (c:InstitutionalClient {id:$cid})-[:HAS_TRADER]->(t:Trader)
          -[:PLACES]->(o:Order)-[:FOR_STOCK]->(s:Stock)
    RETURN o.order_id      AS order_id,
           t.name           AS trader,
           s.symbol         AS stock,
           o.quantity       AS qty,
           o.price          AS price,
           (o.quantity*o.price) AS total_value,
           o.status         AS status,
           o.timestamp      AS ts
    ORDER BY ts DESC
    """
    with driver.session() as s:
        rows = s.execute_read(run_query, cypher, {"cid": client_id})
    return pd.DataFrame(rows)


# ------------------- ② 컴플라이언스 체크 --------------------
def compliance_violations() -> pd.DataFrame:
    """
    현재 DB에 존재하는 두 가지 규칙에 대해 위반 주문을 반환.
      * CR001 – Max Order Size (threshold 필드)
      * CR002 – Restricted Stock List (type='Blacklist')
    결과 컬럼: order_id, rule_id, rule_name, violated_value, threshold
    """
    cypher = """
    // 1) Max Order Size (CR001)
    MATCH (o:Order)-[:CHECKED_AGAINST]->(r:ComplianceRule {rule_id:'CR001'})
    WHERE (o.quantity * o.price) > r.threshold
    RETURN o.order_id AS order_id,
           r.rule_id   AS rule_id,
           r.name      AS rule_name,
           (o.quantity * o.price) AS violated_value,
           r.threshold AS threshold

    UNION

    // 2) Restricted Stock List (CR002) – 여기선 예시로 stock.symbol='JPM'이 blacklist라고 가정
    MATCH (o:Order)-[:FOR_STOCK]->(s:Stock {symbol:'JPM'})
          -[:CHECKED_AGAINST]->(:ComplianceRule {rule_id:'CR002'})
    RETURN o.order_id AS order_id,
           'CR002'   AS rule_id,
           'Restricted Stock List' AS rule_name,
           s.symbol  AS violated_value,
           null      AS threshold
    """
    with driver.session() as s:
        rows = s.execute_read(run_query, cypher)
    return pd.DataFrame(rows)


# ------------------- ③ 시장·섹터 노출도 ------------------
def market_exposure() -> pd.DataFrame:
    """
    마켓·섹터 별 총 주문 금액(=수량×가격)을 집계.
    컬럼: market, sector, total_value
    """
    cypher = """
    MATCH (o:Order)-[:FOR_STOCK]->(s:Stock)-[:TRADED_ON]->(m:Market)
    WITH m.name AS market, s.sector AS sector,
         SUM(o.quantity * o.price) AS total_value
    RETURN market, sector, total_value
    ORDER BY total_value DESC
    """
    with driver.session() as s:
        rows = s.execute_read(run_query, cypher)
    return pd.DataFrame(rows)


# ------------------- 메인 실행 흐름 -----------------------
def main():
    print("\n=== ① 클라이언트별 주문 현황 (IC001) ===")
    df_client = client_orders('IC001')
    print(df_client.head())

    print("\n=== ② 현재 위반 중인 컴플라이언스 규칙 ===")
    df_viol = compliance_violations()
    if df_viol.empty:
        print("⚡️ 위반 건이 없습니다.")
    else:
        print(df_viol)

    print("\n=== ③ 마켓·섹터 노출도 ===")
    df_market = market_exposure()
    print(df_market)

# if __name__ == "__main__":
main()



# --------------------------------------------------------------
# order_workflow_fixed_v5.py
# --------------------------------------------------------------
import os
from datetime import datetime, timezone
from neo4j import GraphDatabase

# --------------------------------------------------------------
# ① Neo4j 연결
# --------------------------------------------------------------
# URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
# USER     = os.getenv("NEO4J_USER",     "neo4j")
# PASSWORD = os.getenv("NEO4J_PASSWORD", "your_password")   # ← 바꾸세요

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


# --------------------------------------------------------------
# ② 헬퍼 (쓰기 / 읽기) – **결과를 즉시 리스트화**  
# --------------------------------------------------------------
def _run_write(cypher: str, params: dict | None = None) -> list[dict]:
    """Write‑transaction → consume Result → return list of dicts."""
    with driver.session() as s:
        def tx_work(tx):
            result = tx.run(cypher, params or {})
            return [record.data() for record in result]   # ← 여기서 바로 소비
        # Neo4j 5.x+ uses execute_write, older versions have write_transaction
        return s.execute_write(tx_work) if hasattr(s, "execute_write") else s.write_transaction(tx_work)


def _run_read(cypher: str, params: dict | None = None) -> list[dict]:
    """Read‑transaction → consume Result → return list of dicts."""
    with driver.session() as s:
        def tx_work(tx):
            result = tx.run(cypher, params or {})
            return [record.data() for record in result]
        return s.execute_read(tx_work) if hasattr(s, "execute_read") else s.read_transaction(tx_work)


# --------------------------------------------------------------
# ③ 주문 생성 – WITH 로 변수 전달 (문법 오류 해결)
# --------------------------------------------------------------
def create_order(
    trader_id: str,
    stock_symbol: str,
    order_type: str,
    quantity: int,
    price: float,
    timestamp: str | None = None,
) -> str:
    ts = timestamp or datetime.now(timezone.utc).isoformat()

    cypher = """
    WITH $traderId    AS traderId,
         $stockSymbol AS sym,
         $orderType   AS typ,
         $qty         AS qty,
         $price       AS price,
         $ts          AS ts

    // ① Order node 생성
    CREATE (o:Order {
        order_id : randomUUID(),
        quantity : qty,
        price    : price,
        status   : 'New',
        created  : ts
    })

    // ② o 를 후속 절에 넘기기
    WITH o, traderId, sym, typ

    // ③ 관계 매핑
    MATCH (t:Trader {id: traderId})
    MATCH (s:Stock  {symbol: sym})
    MATCH (ot:OrderType {type: typ})
    MERGE (t)-[:PLACES]->(o)
    MERGE (o)-[:FOR_STOCK]->(s)
    MERGE (o)-[:HAS_TYPE]->(ot)

    RETURN o.order_id AS order_id
    """
    res = _run_write(
        cypher,
        {
            "traderId": trader_id,
            "stockSymbol": stock_symbol,
            "orderType": order_type,
            "qty": quantity,
            "price": price,
            "ts": ts,
        },
    )
    return res[0]["order_id"]


# --------------------------------------------------------------
# ④ 컴플라이언스 검사 (예시 규칙 CR001, CR002)
# --------------------------------------------------------------
def check_compliance(order_id: str) -> list[dict]:
    cypher = """
    // ----- CR001 : Max Order Size ---------------------------------
    MATCH (o:Order {order_id:$oid})-[:CHECKED_AGAINST]->(r:ComplianceRule {rule_id:'CR001'})
    WITH o, r, (o.quantity * o.price) AS val
    WHERE val > r.threshold
    RETURN o.order_id   AS order_id,
           r.rule_id     AS rule_id,
           r.name        AS rule_name,
           val           AS violated_value,
           r.threshold   AS threshold

    UNION

    // ----- CR002 : Restricted Stock List -------------------------
    MATCH (o:Order {order_id:$oid})-[:FOR_STOCK]->(s:Stock {symbol:'JPM'})
    RETURN o.order_id   AS order_id,
           'CR002'       AS rule_id,
           'Restricted Stock' AS rule_name,
           null          AS violated_value,
           null          AS threshold
    """
    return _run_read(cypher, {"oid": order_id})


# --------------------------------------------------------------
# ⑤ 주문 상태 업데이트 (APOC 가 있든 없든 자동 fallback)
# --------------------------------------------------------------
def update_order_status(order_id: str, new_status: str, comment: str | None = None) -> dict:
    # ---- APOC 사용 버전 ----
    cypher_with_apoc = """
    MATCH (o:Order {order_id:$oid})
    SET o.status = $status
    CALL apoc.do.when(
        $comment IS NOT NULL,
        'SET o.comment = $comment RETURN o',
        '',
        {o:o, comment:$comment}
    ) YIELD value
    RETURN o.order_id AS order_id,
           o.status   AS status,
           o.comment  AS comment
    """

    # ---- APOC 없을 때 fallback ----
    cypher_fallback = """
    MATCH (o:Order {order_id:$oid})
    SET o.status = $status
    RETURN o.order_id AS order_id,
           o.status   AS status
    """

    params = {"oid": order_id, "status": new_status, "comment": comment}
    try:
        # APOC 존재 여부와 관계없이 _run_write가 결과를 리스트화해 반환한다.
        return _run_write(cypher_with_apoc, params)[0]
    except Exception:
        # APOC가 없거나 실행 중 오류가 난 경우 fallback 사용
        return _run_write(cypher_fallback, {"oid": order_id, "status": new_status})[0]


# --------------------------------------------------------------
# ⑥ 전체 워크플로우 (주문 → 검증 → 상태 전환)
# --------------------------------------------------------------
def place_order_workflow(
    trader_id: str,
    stock_symbol: str,
    order_type: str,
    quantity: int,
    price: float,
) -> dict:
    # ① 주문 생성
    order_id = create_order(trader_id, stock_symbol, order_type, quantity, price)
    print(f"✅ 주문 생성 – order_id={order_id}")

    # ② 컴플라이언스 검사
    violations = check_compliance(order_id)

    # ③ 결과에 따라 상태 업데이트 & 반환값 구성
    if violations:
        reason = "; ".join(v["rule_name"] for v in violations)
        update_order_status(order_id, "Rejected", comment=reason)
        print(f"❌ 위반 – {reason}")
        return {"order_id": order_id, "status": "Rejected", "violations": violations}
    else:
        update_order_status(order_id, "Accepted")
        print("✅ 검증 통과 – Accepted")
        return {"order_id": order_id, "status": "Accepted", "violations": []}


# --------------------------------------------------------------
# ⑦ 데모 실행 (두 시나리오)
# --------------------------------------------------------------
def demo():
    
    # 정상 시나리오
    # print("\n--- 정상 주문 테스트 ---")
    # place_order_workflow(
    #     trader_id="TR001",
    #     stock_symbol="AAPL",
    #     order_type="Buy",
    #     quantity=300,       # 300 * 150 = 45 000 < threshold
    #     price=150.0,
    # )

    # # 위반 시나리오 – 금액이 크므로 CR001 초과
    print("\n--- 위반 주문 테스트 ---")
    place_order_workflow(
        trader_id="TR001",
        stock_symbol="AAPL",
        order_type="Buy",
        quantity=2000,      # 2000 * 150 = 300 000 > 예시 threshold 100 000
        price=150.0,
    )

    


# if __name__ == "__main__":
demo()



