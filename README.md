# Citation Corpus Collector

沿论文站的参考文献构建引用语料的采集系统：**FastAPI + SQLite** 采集 API、双采集槽并发、
全量状态持久化、中断自动续采，附带一个含循环引用 / 迟到短路径 / 一次请求失败的本地论文站。

## 快速开始

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| crawler | http://localhost:8000/docs | 交互式 API 文档（Swagger UI），全部演示在此完成 |
| papersite | http://localhost:8080/papers/p0.html | 本地论文站（静态 HTML） |

采集状态保存在命名卷 `crawler_data` 的 SQLite 中，`docker compose restart crawler`
甚至 `down`/`up` 之后仍可续采。

## 演示流程（在 Swagger UI 中操作，或用 curl）

**1. 提交采集作业** — `POST /jobs`：

```json
{"seed_url": "http://papersite:8080/papers/p0.html", "max_depth": 4}
```

种子深度为 0，`max_depth` 取值 0～4。

**2. 观察采集** — `GET /jobs/1`。论文站里 `slow.html` 会延迟 2.5s 响应：
另一个采集槽不会等同层慢页，会先把长链 `c1→c2→c3→X` 抓完（X 先以深度 4 登记）；
慢页到达后揭示短路径 `slow→X`，X 的深度被下调为 2，并沿已存引用边传播，
使原本超深的 `Y`（5→3）、`Z`（4）进入深度范围被**补抓**。最终：

```
status: failed          # broken.html 首次请求返回 500，仍有失败 -> 不得报完成
counts: {done: 13, failed: 1, external: 1, non_html: 1}
by_depth: {0:1, 1:3, 2:6, 3:3, 4:3}     # X 已在深度 2，Y 在 3，Z 在 4
```

**3. 查看失败** — `GET /jobs/1/failures` → `broken.html / HTTP 500`。

**4. 重试续采** — `POST /jobs/1/retry`：失败页重新入队（已采内容保留、不重复登记），
`broken.html` 第二次请求成功，作业转为 `completed`。

**5. 中断续采** — 再建一个作业（`POST /jobs`），在 2.5s 慢页窗口内执行
`docker compose restart crawler`：进程重启后自动把 `fetching` 重置为 `pending`
并恢复全部 running 作业，不漏抓、不重复登记（`papers` 表 `UNIQUE(job_id,url)`、
`citations` 表主键去重）。

**6. 查询语料**：

- `GET /jobs/1/papers`（可按 `status`、`depth` 过滤）— 论文 URL、深度、题名、状态
- `GET /jobs/1/citations`（可按 `src` 过滤）— 引用边

## 演示语料站结构

```
p0 ─┬─> p1 ─┬─> p2 ─┬─> p3 ─> p1          循环引用（登记一次，不重复展开）
    │       │       └─> p4 ─> p5 ─> mailto:（非 http，忽略）
    │       │              └─> slides.pdf  非 HTML，记 non_html
    │       ├─> p3 (p2.html#related 片段已去掉)
    │       ├─> broken.html                首次 500，重试成功（一次请求失败）
    │       └─> http://example.org/...     非同源，记 external 不抓
    ├─> slow ──────────────> X             迟到短路径（延迟 2.5s）：X 深度 4→2
    └─> c1 ─> c2 ─> c3 ─> X ─> Y ─> Z      长链；Y 深度 5→3 补抓，Z 深度 4 补抓
```

## 采集规则实现

- **仅同源、无重定向、静态 HTML**：`httpx` 关闭重定向跟随，3xx 记为失败；
  非 `text/html` 记 `non_html`；非同源 URL 登记为 `external` 不抓取。
- **解析**：`<title>` 为题名，`a.citation` 的 `href` 为引用；相对地址按当前页
  `urljoin` 解析，`urldefrag` 去掉片段。
- **双采集槽**：每作业 2 个 worker 各自认领下一待抓页（按深度、登记序），
  槽空且有待抓页即发起请求，无同层等待。
- **深度即最短距离**：发现更短路径时下调深度并沿已存引用边传播（严格递减、
  有下界，引用环安全），新进入深度范围的论文自动补抓，已抓页面不重复抓取。
- **共同持久化**：页面状态、引用边、待抓进度在同一 SQLite 事务中提交；
  重启时 `fetching → pending` 并恢复所有 running 作业。
- **失败语义**：请求失败仅标记该页 `failed`，已采内容保留；作业存在失败时
  状态为 `failed` 而非 `completed`；`POST /jobs/{id}/retry` 重新入队失败页续采。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 创建作业 `{seed_url, max_depth(0-4)}` |
| GET | `/jobs` / `/jobs/{id}` | 作业列表 / 状态与进度（按状态、深度统计） |
| POST | `/jobs/{id}/retry` | 失败页重新入队并续采 |
| GET | `/jobs/{id}/failures` | 失败 URL 及错误 |
| GET | `/jobs/{id}/papers` | 论文查询（`status`、`depth` 过滤） |
| GET | `/jobs/{id}/citations` | 引用边查询（`src` 过滤） |

## 本地开发（不用 Docker）

```bash
pip install -r crawler/requirements.txt
( cd papersite && python server.py )                       # :8080
( cd crawler && DATABASE_PATH=./crawler.db \
    uvicorn app.main:app --port 8000 )                     # :8000/docs
```
