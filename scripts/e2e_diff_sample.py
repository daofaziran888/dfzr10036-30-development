"""差异样本回填数据集端到端验证（仅用标准库，对真实运行的后端打 HTTP）。

业务叙事：
1. 上传一份含冲突键与独立键的样本批次；
2. 模拟文件传输失败后的重试，验证批次幂等辨认；
3. 查看断点恢复视图：独立键立即可恢复，冲突键被隔离；
4. 隔离区始终保留原始冲突与隔离原因；
5. 补交一笔事件流更正，冲突键核实后才可恢复；
6. 派发合格样本，关联合格样本与源日志起止位点，向数据比对返还受理键值；
7. 输出每个键何时可被恢复消费、恢复用到了哪些日志位置。
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

BATCH_NO = "BATCH-RECON-20261122-01"
SNAPSHOT = "snapshot-external-recon-20261122"
LOG_START = "binlog.001234:891200"
LOG_END = "binlog.001235:100200"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"HTTP {exc.code}: {exc.read().decode('utf-8')}") from exc


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f" -> {detail}" if detail else ""))
    if not condition:
        raise SystemExit(1)


def main() -> None:
    print("== 0. 健康检查 ==")
    health = call("GET", "/api/health")
    check("后端在线", health["ok"])

    print("\n== 1. 上传含冲突键(order-9001)与独立键(order-9002)的样本 ==")
    samples = [
        {"主键名": "order_id", "主键值": "order-9001", "样本值": "88.50",
         "日志位点": "binlog.001234:892001"},
        {"主键名": "order_id", "主键值": "order-9001", "样本值": "99.90",
         "日志位点": "binlog.001234:892005"},
        {"主键名": "order_id", "主键值": "order-9002", "样本值": "256.00",
         "日志位点": "binlog.001234:893100"},
    ]
    imported = call("POST", "/api/diff-sample/imports", {
        "batch_no": BATCH_NO, "source_snapshot": SNAPSHOT,
        "log_start": LOG_START, "log_end": LOG_END,
        "samples": samples,
    })
    check("导入受理成功", imported["ok"], imported["message"])
    batch = imported["batch"]
    check("批次含 3 条样本", batch["样本总数"] == 3, str(batch["样本总数"]))
    check("独立键直接接纳 1 个", batch["已接纳数"] == 1, str(batch["已接纳数"]))
    check("冲突键整键隔离 1 个（含 2 行冲突样本）", batch["隔离数"] == 1, str(batch["隔离数"]))
    check("批次被标记为部分隔离", batch["status"] == "部分隔离", batch["status"])

    print("\n== 2. 传输失败后重试：同批次号应幂等回放，不重复落库 ==")
    retried = call("POST", "/api/diff-sample/imports", {
        "batch_no": BATCH_NO, "source_snapshot": SNAPSHOT,
        "log_start": LOG_START, "log_end": LOG_END,
        "samples": samples,
    })
    check("重试被幂等辨认", retried["ok"] and retried["batch"]["replayed"] is True,
          retried["message"])
    listed = call("GET", "/api/diff_sample?page=1&size=200")
    check("重试后样本仍只有 3 条", listed["total"] == 3, f"total={listed['total']}")

    print("\n== 3. 断点恢复读取差异键回填数据集 ==")
    resume = call("GET", "/api/diff-sample/resume")
    resumable = {item["主键值"] for item in resume["resumable_keys"]}
    blocked = {item["主键值"] for item in resume["blocked_keys"]}
    check("可恢复键只有独立键 order-9002", resumable == {"order-9002"}, str(resumable))
    check("冲突键 order-9001 被阻止恢复", blocked == {"order-9001"}, str(blocked))

    print("\n== 4. 隔离区原始冲突与隔离原因可查 ==")
    quarantine = call("GET", "/api/diff-sample/quarantine")
    check("隔离区保留 2 条冲突痕迹", quarantine["total"] == 2, str(quarantine["total"]))
    reasons = {item["主键值"]: item["隔离原因"] for item in quarantine["items"]}
    check("隔离原因记录两个互相冲突的原值",
          "88.50" in reasons["order-9001"] and "99.90" in reasons["order-9001"],
          reasons["order-9001"])

    print("\n== 5. 冲突键未核实前派发应被拒绝 ==")
    rejected = call("POST", f"/api/diff-sample/batches/{BATCH_NO}/dispatch", {})
    # 有独立键合格时会部分派发，因此这里先验证隔离键不会被受理
    check("接口返回受理结果", rejected["ok"], rejected["message"])
    check("派发只受理合格键、冲突键仍被拦（键级）",
          rejected["dispatch"]["dispatched"] == 1 and rejected["dispatch"]["blocked"] == 1,
          json.dumps(rejected["dispatch"]["blocked_keys"], ensure_ascii=False))
    accepted = {item["主键值"]: item for item in rejected["dispatch"]["accepted_keys"]}
    check("独立键 order-9002 关联源日志起止位点",
          accepted["order-9002"]["日志起始"] == LOG_START
          and accepted["order-9002"]["日志截止"] == LOG_END,
          json.dumps(accepted["order-9002"], ensure_ascii=False))

    print("\n== 6. 补交一笔事件流更正，整键核实冲突键 ==")
    correction = call("POST", "/api/diff-sample/corrections", {
        "key_value": "order-9001", "corrected_value": "99.90",
        "position": "binlog.001235:205800", "source_snapshot": SNAPSHOT,
    })
    check("更正登记并整键核实冲突键", correction["ok"]
          and correction["result"]["verified"] == 1
          and correction["result"]["still_quarantined"] == 0
          and correction["result"]["confirmed_value"] == "99.90",
          json.dumps(correction["result"], ensure_ascii=False))

    resume_after = call("GET", "/api/diff-sample/resume")
    resumable_after = {item["主键值"] for item in resume_after["resumable_keys"]}
    check("核实后 order-9001 进入可恢复集合（order-9002 已派发不在其中）",
          resumable_after == {"order-9001"}, str(resumable_after))
    confirmed = next(item for item in resume_after["resumable_keys"]
                     if item["主键值"] == "order-9001")
    check("可恢复视图携带确认值与更正位点",
          confirmed["确认值"] == "99.90"
          and confirmed["更正位点"] == "binlog.001235:205800",
          json.dumps(confirmed, ensure_ascii=False))

    quarantine_after = call("GET", "/api/diff-sample/quarantine")
    verified_rows = [item for item in quarantine_after["items"]
                     if item["主键值"] == "order-9001"]
    check("核实后两条原始冲突行仍保留且附核实结论",
          len(verified_rows) == 2
          and all("88.50" in item["原始冲突"] for item in verified_rows)
          and all("更正" in item["隔离原因"] for item in verified_rows)
          and all(item["更正位点"] == "binlog.001235:205800" for item in verified_rows),
          verified_rows[0]["隔离原因"])

    print("\n== 7. 再次派发：已核实键带更正位点恢复，向数据比对返还 ==")
    second_dispatch = call("POST", f"/api/diff-sample/batches/{BATCH_NO}/dispatch", {})
    check("已核实键派发成功（按主键去重受理 1 个、无拦截）", second_dispatch["ok"]
          and second_dispatch["dispatch"]["dispatched"] == 1
          and second_dispatch["dispatch"]["blocked"] == 0,
          second_dispatch["message"])
    for item in second_dispatch["dispatch"]["accepted_keys"]:
        check(
            f"键 {item['主键值']} 恢复截止位点覆盖更正位点",
            item["日志截止"] == "binlog.001235:205800",
            f"{item['日志起始']} ~ {item['日志截止']}",
        )
    check("已核实键按确认值 99.90 受理",
          second_dispatch["dispatch"]["accepted_keys"][0]["受理值"] == "99.90",
          str(second_dispatch["dispatch"]["accepted_keys"][0]))

    print("\n== 8. 校验回填任务与数据比对受理记录 ==")
    backfills = call("GET", "/api/backfill_run?page=1&size=200")
    diff_backfills = [row for row in backfills["items"]
                      if str(row.get("任务编号", "")).startswith("BF-DIFF")]
    check("共生成 2 个差异键回填任务（每键一个）", len(diff_backfills) == 2,
          str(len(diff_backfills)))
    verified_bf = [row for row in diff_backfills if row["截止位点"] == "binlog.001235:205800"]
    check("已核实键回填截止到更正位点", len(verified_bf) == 1,
          json.dumps([(r["任务编号"], r["截止位点"]) for r in verified_bf],
                     ensure_ascii=False))

    diffs = call("GET", "/api/data_diff?page=1&size=200")
    returned = [row for row in diffs["items"]
                if str(row.get("任务编号", "")).startswith("DDF-DIFF")]
    check("数据比对收到 2 笔受理返还", len(returned) == 2, str(len(returned)))
    total_accepted = sum(int(row["差异行数"]) for row in returned)
    check("返还受理键值合计 2 个（独立键+核实键）", total_accepted == 2, str(total_accepted))

    print("\n== 9. 各键恢复消费时间线与日志位置汇总 ==")
    timeline = call("GET", "/api/diff-sample/resume")
    print(json.dumps(timeline, ensure_ascii=False, indent=2))
    event_log = call("GET", "/api/diff-sample/event-log")
    check("事件流更正可追溯", event_log["total"] >= 2, str(event_log["total"]))

    print("\n== 10. CSV 文件形式的样本批次同样可导入（独立键直接合格）==")
    csv_batch = "BATCH-RECON-CSV-02"
    csv_content = (
        "主键名,主键值,样本值,日志位点\r\n"
        "order_id,order-9003,17.80,binlog.001235:300100\r\n"
    )
    csv_import = call("POST", "/api/diff-sample/imports", {
        "batch_no": csv_batch, "source_snapshot": SNAPSHOT,
        "log_start": "binlog.001235:300000", "log_end": "binlog.001235:300500",
        "content_type": "csv", "content": csv_content,
    })
    check("CSV 批次受理成功", csv_import["ok"], csv_import.get("message", ""))
    check("CSV 独立键直接接纳", csv_import["batch"]["已接纳数"] == 1,
          str(csv_import["batch"]))
    csv_dispatch = call("POST", f"/api/diff-sample/batches/{csv_batch}/dispatch", {})
    check("CSV 批次可立即派发", csv_dispatch["ok"]
          and csv_dispatch["dispatch"]["dispatched"] == 1,
          csv_dispatch.get("message", ""))

    print("\n== 11. 通用单条登记被拒，必须走批次导入入口 ==")
    single = call("POST", "/api/diff_sample", {
        "values": {"批次编号": "X", "源快照": "s", "主键值": "k",
                   "样本值": "v", "日志位点": "p"},
    })
    check("单条登记返回失败并引导到批次导入", single["ok"] is False
          and "批次导入" in single["message"], single["message"])

    print("\n== 12. 批次列表可回看全部受理概况 ==")
    batches = call("GET", "/api/diff-sample/batches")
    batch_nos = {item["批次编号"] for item in batches["items"]}
    check("批次列表含两个批次", batches["ok"]
          and {BATCH_NO, csv_batch} <= batch_nos, str(batch_nos))

    print("\n== 13. 导入时自动识别事件流中已存在的更正（预置 order-7788）==")
    auto_batch = "BATCH-RECON-AUTO-03"
    auto_import = call("POST", "/api/diff-sample/imports", {
        "batch_no": auto_batch, "source_snapshot": SNAPSHOT,
        "log_start": "binlog.001235:401000", "log_end": "binlog.001235:402000",
        "samples": [
            {"主键名": "order_id", "主键值": "order-7788", "样本值": "119.00",
             "日志位点": "binlog.001235:401200"},
            {"主键名": "order_id", "主键值": "order-7788", "样本值": "120.00",
             "日志位点": "binlog.001235:401300"},
        ],
    })
    check("冲突键在导入阶段即被既有更正自动核实", auto_import["ok"]
          and auto_import["batch"]["已核实数"] == 1
          and auto_import["batch"]["隔离数"] == 0,
          str(auto_import["batch"]))
    auto_resume = call("GET", "/api/diff-sample/resume")
    auto_key = next(item for item in auto_resume["resumable_keys"]
                    if item["主键值"] == "order-7788")
    check("自动核实键确认值与更正位点正确",
          auto_key["确认值"] == "120.00"
          and auto_key["更正位点"] == "binlog.001235:402100",
          json.dumps(auto_key, ensure_ascii=False))

    print("\n全部断言通过：冲突键核实前不可回填，更正核实后带更正位点恢复，"
          "独立键导入即可恢复，批次重试幂等，隔离痕迹全程可查。")


if __name__ == "__main__":
    main()
