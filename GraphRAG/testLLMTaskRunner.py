import os, json
from typing import List, Dict, Any, Optional, Tuple, Callable
from neo4j import GraphDatabase
from openai import OpenAI

# -----------------------------
# 0) 환경설정
# -----------------------------
NEO4J_URI = "neo4j+s://2815798c.databases.neo4j.io" # bolt://localhost:7687 등
NEO4J_USER = "neo4j"
NEO4J_PASS = "M-5hZ5h6Q197RrlzPBI4LaEoiblCblaXfM1ZtsfuRhE"

OPENAI_MODEL = "gpt-4o-mini" # 필요시 변경
client = OpenAI(api_key=api_key)

# 툴 레지스트리: Tool.name -> 파이썬 함수
def VaR_Calculator(context: Dict[str, Any], **tool_args) -> Dict[str, Any]:
  print("[Tool] VaR_Calculator", tool_args)
  # 실제 VaR 계산 로직 연결
  return {"risk_score": 0.97, "method": "historical"}

def OptimizationEngine(context: Dict[str, Any], **tool_args) -> Dict[str, Any]:
  print("[Tool] OptimizationEngine", tool_args)
  # 실제 최적화 엔진 연결
  return {"new_weights": {"SPY": 0.45, "IEF": 0.35, "GLD": 0.20}, "expected_sharpe": 1.1}

TOOL_REGISTRY: Dict[str, Callable[..., Dict[str, Any]]] = {
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

    def fetch_runnable_tasks(self, current_state: List[str]) -> List[Dict[str, Any]]:
      q = """
      WITH $state AS state
      MATCH (t:Task)
      OPTIONAL MATCH (t)-[:HAS_PRECONDITION]->(pre:Precondition)
      WITH t, collect(pre.name) AS pres, state
      WHERE ALL(p IN pres WHERE p IS NULL OR p IN state)
      OPTIONAL MATCH (t)-[:HAS_POSTCONDITION]->(post:Postcondition)
      OPTIONAL MATCH (t)-[:REQUIRES_INPUT]->(inp:InputSlot)
      OPTIONAL MATCH (t)-[:USES_TOOL]->(tool:Tool)
      WITH t, pres, collect(DISTINCT post.name) as postconditions, collect(DISTINCT inp.name) as inputs, collect(DISTINCT tool.name) as tools // Explicitly pass t and pres
      RETURN t {
      .*,
      preconditions: pres,
      postconditions: postconditions,
      inputs: inputs,
      tools: tools
      } AS task
      """
      with self.driver.session() as s:
        recs = s.run(q, state=current_state).data()
      return [r["task"] for r in recs]


# -----------------------------
# 3) LLM 헬퍼
# -----------------------------
SYSTEM = (
  "You are an execution planner for a financial workflow. "
  "You must output strict JSON with keys: task_name, tool_plan, rationale. "
  "tool_plan is an array of {tool_name, tool_args}."
)

def llm_choose_task(
  runnable_tasks: List[Dict[str, Any]],
  current_state: List[str],
  available_inputs: List[str],
  context: Dict[str, Any],
) -> Dict[str, Any]:
  # 모델에 전달할 압축 컨텍스트
  task_specs = [
    {
      "name": t.get("name"),
      "priority": t.get("priority", 1000),
      "pre": [p for p in t.get("preconditions", []) if p],
      "post": [p for p in t.get("postconditions", []) if p],
      "inputs": [i for i in t.get("inputs", []) if i],
      "tools": [u for u in t.get("tools", []) if u],
    }
    for t in runnable_tasks
  ]
  user_msg = {
    "role": "user",
    "content": (
      "Choose next task and tool plan.\n"
      "Hard constraints:\n"
      " - Use only task names from runnable_tasks.\n"
      " - All required inputs must be in available_inputs.\n"
      " - Prefer tasks whose postconditions are not yet in current_state.\n"
      " - If multiple candidates, use lower priority first.\n"
      "Return strict JSON: "
      '{"task_name": "...", "tool_plan":[{"tool_name":"...","tool_args":{...}}], "rationale":"..."}'
    )
  }
  data_msg = {
    "role": "user",
    "content": json.dumps(
    {
      "runnable_tasks": task_specs,
      "current_state": current_state,
      "available_inputs": available_inputs,
      "context_snapshot": {k: context[k] for k in list(context)[:20]}, # 경량 스냅샷
    },
    ensure_ascii=False,
    ),
  }

  resp = client.chat.completions.create(
    model=OPENAI_MODEL,
    temperature=0,
    response_format={"type": "json_object"},
    messages=[
      {"role": "system", "content": SYSTEM},
      user_msg,
      data_msg,
    ],
  )
  try:
    parsed = json.loads(resp.choices[0].message.content)
  except Exception as e:
    raise RuntimeError(f"LLM JSON parse error: {e}\n{resp.choices[0].message.content}")
  return parsed


# -----------------------------
# 4) 검증기(온톨로지 제약 반영)
# -----------------------------
def validate_llm_plan(
  llm_plan: Dict[str, Any],
  runnable_tasks: List[Dict[str, Any]],
  current_state: List[str],
  available_inputs: List[str],
) -> Dict[str, Any]:
  names = {t["name"]: t for t in runnable_tasks}
  task_name = llm_plan.get("task_name")
  if task_name not in names:
    raise ValueError(f"Invalid task_name: {task_name}")

  task = names[task_name]

  # 입력 충족
  req_inputs = [i for i in task.get("inputs", []) if i]
  missing_inputs = [i for i in req_inputs if i not in available_inputs]
  if missing_inputs:
    raise ValueError(f"Missing inputs: {missing_inputs}")

  # 아직 충족되지 않은 postcondition이 있는지 확인
  posts = [p for p in task.get("postconditions", []) if p]
  if posts:
    unmet_posts = [p for p in posts if p not in current_state]
    if not unmet_posts:
      raise ValueError("All postconditions already satisfied for chosen task.")

  # 툴 검증
  plan_tools = llm_plan.get("tool_plan", [])
  allowed_tools = set([u for u in task.get("tools", []) if u])
  for step in plan_tools:
    tn = step.get("tool_name")
    if tn not in allowed_tools:
      raise ValueError(f"Tool not allowed for task: {tn}")

  return {"task": task, "tool_plan": plan_tools}



# -----------------------------
# 5) 실행기
# -----------------------------
def execute_task_with_plan(
  task: Dict[str, Any],
  tool_plan: List[Dict[str, Any]],
  context: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[str]]:
  tool_outputs = {}
  # LLM 순서대로 툴 실행
  for step in tool_plan:
    tool_name = step.get("tool_name")
    tool_args = step.get("tool_args", {}) or {}
    fn = TOOL_REGISTRY.get(tool_name)
    if fn is None:
      raise RuntimeError(f"Tool not registered: {tool_name}")
    out = fn(context, **tool_args)
    tool_outputs[tool_name] = out
    # 결과를 컨텍스트에 축적
    context[f"{tool_name}_output"] = out

  # 태스크 완료 → postconditions 달성
  new_posts = [p for p in task.get("postconditions", []) if p]
  return tool_outputs, new_posts



current_state = ["PortfolioUpdatedWithin7Days"]
available_inputs = ["HoldingsData", "MarketVolatilityIndex"]
shared_context = {
  "portfolio_id": "A123",
  "mkt_volatility": 27.3,
  "holdings": {"SPY": 0.5, "IEF": 0.3, "GLD": 0.2},
  # 추가 컨텍스트 가능
}

kg = KGClient(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
try:
  for step in range(5):
    runnable = kg.fetch_runnable_tasks(current_state)
    if not runnable:
      print("[Engine] No runnable tasks.")
      break

    # 1) LLM에 선택 요청
    proposal = llm_choose_task(runnable, current_state, available_inputs, shared_context)
    print(f"[LLM] Proposal: {json.dumps(proposal, ensure_ascii=False)}")

    # 2) 온톨로지 제약으로 검증
    validated = validate_llm_plan(proposal, runnable, current_state, available_inputs)

    # 3) 실행
    task = validated["task"]
    plan = validated["tool_plan"]
    print(f"[Engine] Execute Task: {task['name']} | Plan: {plan}")
    outputs, new_posts = execute_task_with_plan(task, plan, shared_context)
    print(f"[Engine] Tool outputs: {outputs}")
    print(f"[Engine] New postconditions: {new_posts}")

    # 4) 상태 갱신
    for p in new_posts:
      if p not in current_state:
        current_state.append(p)

    # 5) 간단한 종료 조건 예시
    if "WeightsOptimized" in current_state:
      print("[Engine] Terminal state reached.")
      break

  print("[Engine] Final state:", current_state)

finally:
  kg.close()
