# Codex Reflow

> No More "error decoding response body"

[中文](#中文) · [English](#english)

## 中文

Codex 断流自动续跑工具。检测到以下错误并确认该轮最终失败后，向对应会话发送「继续」：

```text
stream disconnected before completion: Transport error: network error: error decoding response body
```

**Windows · Python 3.10+ · 无第三方依赖**

### 使用

1. 下载本仓库，打开 Codex 桌面应用。
2. 复制 `config.example.json` 为 `config.json`，将 `caller_thread_id` 改为你已授权自动续跑的 Codex 会话 UUID。可在该会话中让 Codex 返回 ID，并明确授权本工具在目标错误后自动发送「继续」。
3. 双击 `start.cmd` 后台启动，双击 `stop.cmd` 停止。

查看状态：`python -X utf8 monitor.py status`。修改配置后重新启动。

默认每 **0.5 秒**检查；最终失败后等待 **1 秒**发送；同一会话冷却 **5 秒**，连续最多 **3 次**。发送超时 **10 秒**，运行确认 **15 秒**。可修改 `message` 自定义续跑消息。

程序等待 Codex 自身重试结束，跳过手动停止、已有新输入和历史失败；支持去重、次数限制与新轮次确认，不抢占鼠标键盘。达到上限后，成功完成一轮会清零；也可停止程序后运行 `python -X utf8 monitor.py reset` 手动解除限制。

仅支持本机 Windows Codex 会话，需保持桌面应用打开。使用桌面工具接口及本地数据结构，Codex 升级后可能需要适配。实际配置、日志和运行状态保留在本地。

## English

Automatically resume Codex after the stream error shown above. Reflow waits for a turn to fail, then sends a continuation message to the affected conversation.

**Windows · Python 3.10+ · No third-party dependencies**

### Quick start

1. Download this repository and open the Codex desktop app.
2. Copy `config.example.json` to `config.json`. Set `caller_thread_id` to the UUID of a Codex conversation where you have authorized automatic continuation. You can ask Codex in that conversation for its ID and explicitly authorize Reflow to send continuation messages after the matching error.
3. Double-click `start.cmd` to run in the background, or `stop.cmd` to stop.

Check status with `python -X utf8 monitor.py status`. Restart after editing the configuration.

Defaults: check every **0.5 seconds**, wait **1 second** after failure, use a **5-second** cooldown per conversation, and attempt up to **3** consecutive continuations. Sending times out after **10 seconds**; confirmation waits **15 seconds**. Change `message` to `Continue` if preferred.

Reflow waits for Codex's own retries, skips manual stops, newer input and historical failures, and prevents duplicate sends. It checks for a new turn without taking over your mouse or keyboard. A successful turn resets the retry count; to reset it manually, stop Reflow and run `python -X utf8 monitor.py reset`.

Local Windows conversations only; keep the Codex desktop app open. Reflow uses desktop tools and local data formats, so Codex updates may require adjustments. Your configuration, logs and runtime state stay local.

## 验证 / Checks

```powershell
python -X utf8 monitor.py check
python -X utf8 -m unittest -v test_monitor
```

测试使用临时数据，不向真实会话发送消息。Tests use temporary data and never send messages to real conversations.
