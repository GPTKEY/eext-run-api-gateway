# 01_20260806 Gateway 重连可靠性修改记录

## 1. 修改目标

修复 Gateway 在连续 5 次扫描失败后永久停止重连的问题，并使自动连接开关、手动停止、心跳断线和 Bridge 晚启动场景具有一致、可观察、可恢复的行为。

## 2. 根因

原实现存在以下问题：

1. `MAX_RETRIES=5`，达到上限后 `scanAndConnect()` 直接返回，后续不再恢复；
2. 所有失败固定等待 3 秒，没有封顶退避和随机抖动；
3. `toggleAutoConnect()` 只修改持久化配置，没有同步更新内存状态，也没有即时启动或停止当前连接流程；
4. 手动停止、自动连接关闭和暂时断线没有明确区分；
5. 每轮扫描失败都显示 Toast，Bridge 长时间未启动时会持续打扰用户；
6. 心跳超时依赖未受统一管理的延迟回调，状态和错误信息不可观察；
7. About 对话框只能显示连接或断开，无法区分扫描、等待、退避和手动停止。

## 3. 修改范围

### 3.1 `src/index.ts`

1. 新增连接状态：
   - `disabled`；
   - `manual-stopped`；
   - `scanning`；
   - `connecting`；
   - `connected`；
   - `waiting-bridge`；
   - `backoff`；
   - `error`。
2. 新增连接意图：
   - `auto`；
   - `manual`；
   - `disabled`；
   - `manual-stopped`。
3. 删除固定最大重试次数；
4. 新增 1、2、4、8、16、30 秒封顶指数退避，并添加 ±20% 随机抖动；
5. Bridge 未找到提示限频为 60 秒最多一次；
6. 自动连接开关即时影响当前连接流程；
7. 手动重新连接在自动连接关闭时仍可建立并维持 `manual` 连接；
8. 手动停止只停止当前运行会话，不修改下次启动使用的持久化自动连接设置；
9. 跟踪心跳超时定时器，收到 `pong` 后立即清除；
10. 心跳发送失败和超时统一进入 `handleConnectionLost()`；
11. 使用单调递增 `connectionSessionId` 隔离旧扫描和迟到回调；
12. 扩展 MessageBus 状态，返回状态、意图、重试次数、下次重试时间和最近错误；
13. About 对话框显示详细连接诊断信息。

### 3.2 `locales/en.json`、`locales/zh-Hans.json`

增加连接成功和带重试次数的等待提示。

### 3.3 `CHANGELOG.md`

记录本轮尚未发布的重连可靠性变化。

## 4. 修改后的关键流程

```text
扩展启动
→ 读取 autoConnectEnabled
→ auto=true：进入 scanning
→ 扫描 49620-49629
→ 握手成功：connected + heartbeat
→ 未找到：指数退避 + 继续扫描
→ Bridge 后启动：自动发现并连接
```

```text
用户禁用自动连接
→ 立即取消扫描、连接、心跳和重试
→ state=disabled
→ 不再后台连接
```

```text
自动连接关闭后点击重新连接
→ intent=manual
→ 开始扫描
→ 断线后继续恢复
→ 点击停止连接后结束本次 manual 会话
```

## 5. 安全与并发边界

1. 每次启动新连接流程都会更新 `connectionSessionId`；
2. 旧会话的超时和消息回调只能结束自身 Promise，不得关闭新连接；
3. 所有重试和心跳定时器在停止、禁用和重连前清理；
4. 重试指数被限制在 30 秒，避免数值溢出和极端等待；
5. 本轮不改变 Bridge 消息协议中的 `execute/result/error/ping/pong/handshake`；
6. 本轮不增加 EasyEDA 文档写操作。

## 6. 验证

已完成：

1. TypeScript 严格模式静态检查；
2. JSON 本地化内容结构检查；
3. 保持单一 `src/index.ts` 入口；
4. 保持 IIFE 构建结构和现有扩展菜单不变。

仍需实机验证：

1. Bridge 未启动超过 5 轮后仍持续重试；
2. Bridge 后启动后自动连接；
3. Bridge 重启后自动恢复；
4. 自动连接开关即时启停；
5. 自动连接关闭时手动连接可用；
6. 手动停止后本次会话不再重试；
7. 多窗口环境中 MessageBus 状态与控制行为；
8. 心跳超时后的恢复过程；
9. Toast 限频实际表现。

## 7. 回滚

如实机验证发现兼容性问题，可回滚 `src/index.ts`、两份 locale 和 CHANGELOG 的本轮提交；计划文档可保留作为问题记录。