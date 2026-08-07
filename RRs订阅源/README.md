# RRS 服务 — Python 实现任务

## 目标

写一个 Python RSS 聚合服务，读取 `rrs_config.json`，定时抓取所有 RSS 源，按分类输出合并后的 RSS feed（约 10 个 XML 文件）。

## 功能要求

### 1. 读取配置
- 读取 `rrs_config.json` 中 `sources` 下每个分类的 feeds
- 每个分类输出一个 `output_feed` 文件（XML）

### 2. Feed 拉取
- 用 `feedparser` 拉取每个 RSS/Atom feed
- 超时 10 秒
- 失败跳过，不中断

### 3. 输出
- 输出标准 RSS 2.0 XML
- 每 feed 最多 50 条 item
- 按 `pubDate` 降序排列
- 去重：同 URL 的 item 只保留最新一条
- item 结构：`<title>`, `<link>`, `<description>`, `<pubDate>`, `<guid>`, `<source>`

### 4. Web 服务
- Flask 或 FastAPI 提供 HTTP 端点
- `GET /` → feed 列表页面
- `GET /feeds/{output_feed}` → 返回对应 RSS XML（Content-Type: application/rss+xml）
- `GET /health` → 健康检查

### 5. 定时更新
- 每 30 分钟自动刷新全部 feeds（可选 threading 定时器或 APScheduler）
- 手动触发刷新端点：`POST /refresh`

### 6. 部署
- Railway / VPS 部署
- 提供 `requirements.txt`
- 监听端口 `$PORT` 环境变量（Railway 默认）

## 配置结构（rrs_config.json）

```json
{
  "sources": {
    "deals": {
      "name": "福利·羊毛·Deals",
      "output_feed": "deals.xml",
      "feeds": [...]
    },
    "tech_news": { ... },
    "forums": { ... },
    ...
  }
}
```

每个 feed 条目格式：
```json
{"name": "源名称", "url": "https://...", "source": "来源描述"}
```

## 输出汇总

| feed | 内容 | 源数 |
|:----|:----|:----:|
| deals.xml | 福利·羊毛·Deals | 4 |
| tech.xml | 科技资讯 | 13 |
| forums.xml | 技术论坛 | 7 |
| selfhosted.xml | 自部署·HomeLab | 4 |
| hn.xml | Hacker News 精选 | 3 |
| ai.xml | AI 前沿 | 1 |
| youtube.xml | YouTube 频道 | 12 |
| rsshub.xml | 需 RSSHub 转换 | 3 |