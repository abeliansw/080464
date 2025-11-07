아래의 파이썬 코드를 실행해줘


# pip install neo4j
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional, Tuple, Callable

# -----------------------------
# 0) 환경설정
# -----------------------------
NEO4J_URI = "neo4j+s://<your-endpoint>" # bolt://localhost:7687 등
NEO4J_USER = "neo4j"
NEO4J_PASS = "<password>"

# 툴 레지스트리: Tool.name -> 파이썬 함수
def VaR_Calculator(context: Dict[str, Any]) -> Dict[str, Any]:
# TODO: 실제 VaR 계산 로직 연결
print("[Tool] VaR_Calculator called with", context)
return {"risk_score": 0.97, "method": "historical"}

def OptimizationEngine(context: Dict[str, Any]) -> Dict[str, Any]:
# TODO: 실제 최적화 엔진 연결
print("[Tool] OptimizationEngine called with", context)
return {"new_weights": {"SPY": 0.45, "IEF": 0.35, "GLD": 0.20}, "expected_sharpe": 1.1}

TOOL_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
"VaR_Calculator": VaR_Calculator,
"OptimizationEngine": OptimizationEngine,
}

# -----------------------------
# 1) Cypher 유틸
# -----------------------------
class KGClient:
def __init__(self, uri: str, user: str, password: str):
self.driver = GraphDatabase.driver(uri, auth=(user, password))

def close(self):
self.driver.close()

# 모든 태스크와 그 전제·후조건·입력·도구를 한 번에 조회
def fetch_tasks(self) -> List[Dict[str, Any]]:
q = """
MATCH (t:Task)
OPTIONAL MATCH (t)-[:HAS_PRECONDITION]->(pre:Precondition)
OPTIONAL MATCH (t)-[:HAS_POSTCONDITION]->(post:Postcondition)
OPTIONAL MATCH (t)-[:REQUIRES_INPUT]->(inp:InputSlot)
OPTIONAL MATCH (t)-[:USES_TOOL]->(tool:Tool)
RETURN t {.*,
preconditions: collect(DISTINCT pre.name),
postconditions: collect(DISTINCT post.name),
inputs: collect(DISTINCT inp.name),
tools: collect(DISTINCT tool.name)
} AS task
"""
with self.driver.session() as s:
recs = s.run(q).data()
return [r["task"] for r in recs]

# 현재 상태에서 실행 가능한 태스크만 필터 (모든 전제 충족)
def fetch_runnable_tasks(self, current_state: List[str]) -> List[Dict[str, Any]]:
q = """
WITH $state AS state
MATCH (t:Task)
OPTIONAL MATCH (t)-[:HAS_PRECONDITION]->(pre:Precondition)
WITH t, collect(pre.name) AS pres, state
WHERE ALL(p IN pres WHERE p IS NULL OR p IN state) // 전제가 없거나 전부 충족
OPTIONAL MATCH (t)-[:HAS_POSTCONDITION]->(post:Postcondition)
OPTIONAL MATCH (t)-[:REQUIRES_INPUT]->(inp:InputSlot)
OPTIONAL MATCH (t)-[:USES_TOOL]->(tool:Tool)
RETURN t {.*,
preconditions: pres,
postconditions: collect(DISTINCT post.name),
inputs: collect(DISTINCT inp.name),
tools: collect(DISTINCT tool.name)
} AS task
"""
with self.driver.session() as s:
recs = s.run(q, state=current_state).data()
return [r["task"] for r in recs]

# -----------------------------
# 2) 정책: 다음 태스크 선택 로직
# -----------------------------
def choose_next_task(
runnable_tasks: List[Dict[str, Any]],
current_state: List[str],
available_inputs: List[str],
) -> Optional[Dict[str, Any]]:
"""
우선순위 예시:
1) 아직 달성되지 않은 postcondition을 생성하는 태스크 우선
2) 필요한 입력(inputs)을 모두 갖춘 태스크만
3) t.priority(낮을수록 우선) 속성이 있으면 정렬
"""
candidates = []
for t in runnable_tasks:
# 입력 충족 확인
req_inputs = [i for i in t.get("inputs", []) if i]
if not all(i in available_inputs for i in req_inputs):
continue

# 여전히 미달성된 postcondition이 있는지
posts = [p for p in t.get("postconditions", []) if p]
unmet_posts = [p for p in posts if p not in current_state]
if posts and not unmet_posts:
# postcondition이 이미 모두 만족이면 스킵
continue

candidates.append(t)

if not candidates:
return None

# priority가 있으면 적용. 없으면 그대로.
candidates.sort(key=lambda x: x.get("priority", 1000))
return candidates[0]

# -----------------------------
# 3) 실행기: 태스크 실행과 상태 갱신
# -----------------------------
def execute_task(task: Dict[str, Any], context: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
"""
- 연결된 Tool들을 순차 호출
- 결과와 함께 postcondition 반환
"""
tools = [t for t in task.get("tools", []) if t]
tool_outputs = {}
for tool_name in tools:
tool_fn = TOOL_REGISTRY.get(tool_name)
if tool_fn is None:
raise RuntimeError(f"Tool not registered: {tool_name}")
out = tool_fn(context)
tool_outputs[tool_name] = out
# 컨텍스트에 누적 반영 가능
context.update({f"{tool_name}_output": out})

# 태스크 완료로 간주하고 postcondition 달성
new_posts = [p for p in task.get("postconditions", []) if p]
return tool_outputs, new_posts

# -----------------------------
# 4) 메인 루프 예시
# -----------------------------
if __name__ == "__main__":
# 초기 상태와 입력 가정
current_state = ["PortfolioUpdatedWithin7Days"] # Precondition 충족 예시
available_inputs = ["HoldingsData", "MarketVolatilityIndex"] # 인풋 보유
shared_context = {
"portfolio_id": "A123",
"mkt_volatility": 27.3,
"holdings": {"SPY": 0.5, "IEF": 0.3, "GLD": 0.2}
}

kg = KGClient(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
try:
# 루프: 다음 태스크 찾고 실행
for step in range(5):
runnable = kg.fetch_runnable_tasks(current_state)
next_task = choose_next_task(runnable, current_state, available_inputs)

if not next_task:
print("[Engine] No runnable task. Stop.")
break

print(f"[Engine] Step {step} -> Execute Task: {next_task.get('name')}")
outputs, new_posts = execute_task(next_task, shared_context)
print(f"[Engine] Tool outputs: {outputs}")
print(f"[Engine] New postconditions: {new_posts}")

# 상태 갱신
for p in new_posts:
if p not in current_state:
current_state.append(p)

# 예: 특정 종결 상태 도달 시 종료
if "RiskScoreUpdated" in current_state and next_task.get("name") == "RebalancePortfolio":
print("[Engine] Rebalance done. Stop.")
break

print("[Engine] Final state:", current_state)

finally:
kg.close()
