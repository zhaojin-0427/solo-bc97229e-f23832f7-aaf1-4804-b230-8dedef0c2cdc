# Citation Corpus Collector

沿论文站的参考文献构建引用语料的采集系统：**FastAPI + SQLite** 采集 API、双采集槽并发、
全量状态持久化、中断自动续采，附带一个含循环引用 / 迟到短路径 / 一次请求失败的本地论文站。

新增**发布 API**：按作业在后台生成不可变 ZIP 快照（`papers.csv` / `citations.csv` /
`meta.json`），供学术计量分析的离线程序消费，同一份语料可凭包号重复下载、逐字节复跑。

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

**7. 发布不可变 ZIP（可在采集进行中调用）** — `POST /jobs/1/publications`，
立即返回包号（`id`），后台线程开始构建。`GET /publications/{id}` 轮询进度：

```json
{"status": "running", "stage": "papers", "progress": 42,
 "papers_exported": 7, "citations_exported": 0}
```

完成后 `status=completed`，`file_name=publication_1.zip`，并给出 `download_url`。
构建期间采集、重试、查询全部继续可用。

**8. 下载与复跑** — `GET /publications/1/download`（Swagger 中可直接下载，
或 `curl -o p1.zip`）。ZIP 内三个文件：

| 文件 | 内容 |
|---|---|
| `papers.csv` | 列 `url,depth,title,status`（URL、深度、题名、状态），按深度、登记序 |
| `citations.csv` | 列 `src_url,dst_url`（起点 URL、终点 URL），按起点、终点序 |
| `meta.json` | `job_id`、`seed_url`、`max_depth`、`job_status`、`papers_rows`、`citations_rows`、生成时间与列定义 |

同一包号重复下载得到**逐字节相同**的 ZIP（包不可变）；重试补抓后再发布得到**新包号**，
两个包分别固化各自时刻的语料（例如重试前的包里 `broken.html` 为 `failed`、题名为空，
重试后的包里为 `done` 且带题名）。`meta.json` 的两个行数与 CSV 实际数据行数严格一致。

**9. 发布错误查询** — 失败的包 `status=failed` 并带 `error` 字段（`GET /publications/{id}`
或 `GET /publications?job_id=1`）；未完成的包不开放下载（返回 409 并说明当前阶段）。

## 发布语义与可靠性保证

- **同一完整已提交状态**：每个包由发布线程的**独立只读连接**构建，一条只读事务
  贯穿作业行、两表计数与全部扫描。WAL 下该事务固定一个快照，后台开始读取后采集侧
  提交的补抓 / 重试 / 深度传播一律不会混入；三表（含 `meta.json` 行数）天然对应同一状态。
- **不阻塞业务库**：只读事务不获取写锁，扫描跑在独立连接上；进度更新走主连接的
  短事务，扫描与打包不持有业务写事务，采集的 `IMMEDIATE` 写入照常提交。
- **内存恒定**：两张表均以服务端游标 `fetchmany(1000)` 分批读出并直接流式写入 ZIP
  条目，Python 侧不积累全量行，内存占用不随总论文/引用行数增长（测试以 1.6 万行验证）。
- **先落盘后登记**：先写唯一临时文件（`O_EXCL`）→ flush + `fsync` → 同文件系统
  原子 `rename` → 目录 `fsync` → 才在单个事务里登记 `completed` 与文件名。
  写盘或登记失败都只会留下 `failed` 记录并清理残件，不可能出现“登记成功却缺文件”。
- **重启恢复**：启动时，未到终态的发布任务一律标记 `failed`（`error=interrupted by restart`）
  并删除其临时/孤儿文件；已 `completed` 且文件存在的包原样保留、继续可下载；
  若已完成记录的文件确实丢失，则把该记录转为 `failed` 并注明原因，绝不静默误删其他包。

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
| POST | `/jobs/{id}/publications` | 后台发布不可变 ZIP 快照，返回包号 |
| GET | `/publications` | 包列表（可按 `job_id` 过滤） |
| GET | `/publications/{id}` | 包进度、行数、文件名、错误信息 |
| GET | `/publications/{id}/download` | 按包号下载 ZIP（可重复，字节不变） |

## 测试

发布引擎带一套集成测试（快照隔离、流式内存、重复下载、错误守卫、重启恢复）：

```bash
python -m unittest discover -s tests -v      # 需先 pip install -r crawler/requirements.txt
```

## 本地开发（不用 Docker）

```bash
pip install -r crawler/requirements.txt
( cd papersite && python server.py )                       # :8080
( cd crawler && DATABASE_PATH=./crawler.db \
    PACKAGES_DIR=./packages \
    uvicorn app.main:app --port 8000 )                     # :8000/docs
```
