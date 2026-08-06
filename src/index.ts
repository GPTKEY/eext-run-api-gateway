/**
 * EasyEDA API Gateway 扩展
 *
 * 为 AI 编程工具提供 EasyEDA Pro 运行时 WebSocket 网关。
 * 扩展启动后扫描本机 49620-49629 端口，完成 Bridge 身份握手后维持连接。
 *
 * 当前职责：
 * 1. 发现并连接 easyeda-api Bridge；
 * 2. 维持心跳，并在 Bridge 晚启动、重启或短暂中断后持续恢复；
 * 3. 接收并执行 Bridge 下发的 EasyEDA API 代码；
 * 4. 返回执行结果、错误和窗口会话标识；
 * 5. 为后续事件通知、文件传输和受控事务保留稳定连接基础。
 */
import * as extensionConfig from '../extension.json';

// ─── 配置 ───────────────────────────────────────────────────────────
const WS_ID = 'ai-bridge';
const PORT_START = 49620;
const PORT_END = 49629;
const SERVICE_ID = 'easyeda-bridge';
const INITIAL_RETRY_DELAY_MS = 1000;
const MAX_RETRY_DELAY_MS = 30000;
const RETRY_JITTER_RATIO = 0.2;
const RETRY_TOAST_COOLDOWN_MS = 60000;
const HEARTBEAT_INTERVAL_MS = 15000;
const HEARTBEAT_TIMEOUT_MS = 5000;
const CONNECTION_TIMEOUT_MS = 1500; // 单端口连接与握手的最大等待时间
const STORAGE_KEY_AUTO_CONNECT = 'autoConnectEnabled';
const MBUS_TOPIC_STATUS = 'api-gateway-status';
const MBUS_TOPIC_CONTROL = 'api-gateway-control';

// ─── 连接状态模型 ────────────────────────────────────────────────────

type GatewayConnectionState =
	| 'disabled'
	| 'manual-stopped'
	| 'scanning'
	| 'connecting'
	| 'connected'
	| 'waiting-bridge'
	| 'backoff'
	| 'error';

type GatewayConnectionIntent = 'disabled' | 'manual-stopped' | 'auto' | 'manual';

interface GatewayConnectionStatus {
	connected: boolean;
	connecting: boolean;
	state: GatewayConnectionState;
	intent: GatewayConnectionIntent;
	port: number | null;
	windowId: string | null;
	autoConnectEnabled: boolean;
	retryAttempt: number;
	nextRetryAt: number | null;
	lastError: string | null;
	lastConnectedAt: number | null;
	stateChangedAt: number;
}

interface GatewayControlRequest {
	command: 'reconnect' | 'stop';
}

interface GatewayControlResponse {
	handled: boolean;
	connected: boolean;
	windowId: string | null;
}

interface BridgeMessage {
	type: 'execute' | 'ping' | 'pong' | 'handshake' | 'result' | 'error';
	id?: string;
	code?: string;
	service?: string;
	result?: unknown;
	error?: string;
	timestamp?: number;
}

let currentPort: number | null = null;
let handshakeVerified = false;
let retryTimer: ReturnType<typeof setTimeout> | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null;
let heartbeatPending = false;
let autoConnectEnabled = true;
let retryAttempt = 0;
let nextRetryAt: number | null = null;
let windowId: string | null = null;
let isConnecting = false;
let connectionSessionId = 0;
let messageBusRegistered = false;
let connectionState: GatewayConnectionState = 'disabled';
let connectionIntent: GatewayConnectionIntent = 'disabled';
let stateChangedAt = Date.now();
let lastError: string | null = null;
let lastConnectedAt: number | null = null;
const toastLastShownAt = new Map<string, number>();

/**
 * 判断当前会话是否要求 Gateway 持续维持连接。
 *
 * - auto：来自持久化的自动连接设置；
 * - manual：用户手动点击“重新连接”，即使自动连接关闭也持续重连；
 * - disabled/manual-stopped：不允许后台再次发起连接。
 */
function shouldMaintainConnection(): boolean {
	return connectionIntent === 'auto' || connectionIntent === 'manual';
}

function getIdleStateForIntent(): GatewayConnectionState {
	return connectionIntent === 'manual-stopped' ? 'manual-stopped' : 'disabled';
}

/**
 * 统一记录连接状态变化，供 About 对话框和 MessageBus 状态查询使用。
 */
function setConnectionState(state: GatewayConnectionState, reason?: string): void {
	if (connectionState !== state) {
		console.info(`[API-Gateway] State: ${connectionState} -> ${state}${reason ? ` (${reason})` : ''}`);
		connectionState = state;
		stateChangedAt = Date.now();
	}
	else if (reason) {
		console.debug(`[API-Gateway] State ${state}: ${reason}`);
	}
}

/**
 * 对周期性状态提示进行限频，避免 Bridge 长时间未启动时反复弹出 Toast。
 */
function showRateLimitedToast(key: string, message: string, cooldownMs = RETRY_TOAST_COOLDOWN_MS): void {
	const now = Date.now();
	const lastShownAt = toastLastShownAt.get(key) ?? 0;
	if (now - lastShownAt < cooldownMs) {
		return;
	}
	toastLastShownAt.set(key, now);
	eda.sys_Message.showToastMessage(message);
}

/**
 * 获取当前连接状态（供 MessageBus RPC 与 About 对话框调用）。
 */
function getConnectionStatus(): GatewayConnectionStatus {
	return {
		connected: handshakeVerified,
		connecting: isConnecting || connectionState === 'scanning' || connectionState === 'connecting',
		state: connectionState,
		intent: connectionIntent,
		port: currentPort,
		windowId,
		autoConnectEnabled,
		retryAttempt,
		nextRetryAt,
		lastError,
		lastConnectedAt,
		stateChangedAt,
	};
}

function ensureMessageBusServices(): void {
	if (messageBusRegistered) {
		return;
	}

	eda.sys_MessageBus.rpcService(MBUS_TOPIC_STATUS, () => getConnectionStatus());
	eda.sys_MessageBus.rpcService(MBUS_TOPIC_CONTROL, (request?: GatewayControlRequest): GatewayControlResponse => {
		if (request?.command === 'reconnect') {
			performReconnect();
		}
		else if (request?.command === 'stop') {
			performStopConnection(false);
		}

		return {
			handled: true,
			connected: handshakeVerified,
			windowId,
		};
	});

	messageBusRegistered = true;
}

function nextConnectionSessionId(): number {
	connectionSessionId += 1;
	return connectionSessionId;
}

function isConnectionSessionActive(sessionId: number): boolean {
	return sessionId === connectionSessionId;
}

function closeWebSocket(): void {
	try {
		eda.sys_WebSocket.close(WS_ID);
	}
	catch {
		// 关闭动作是尽力清理；连接尚未注册或已经关闭时无需上抛。
	}
}

function clearHeartbeatTimeout(): void {
	if (heartbeatTimeoutTimer) {
		clearTimeout(heartbeatTimeoutTimer);
		heartbeatTimeoutTimer = null;
	}
}

function stopHeartbeat(): void {
	if (heartbeatTimer) {
		clearInterval(heartbeatTimer);
		heartbeatTimer = null;
	}
	clearHeartbeatTimeout();
	heartbeatPending = false;
}

function clearRetryTimer(): void {
	if (retryTimer) {
		clearTimeout(retryTimer);
		retryTimer = null;
	}
	nextRetryAt = null;
}

/**
 * 取消当前扫描、握手、心跳和重试流程。
 * 通过递增 sessionId，使旧回调即使迟到也无法关闭或覆盖新连接。
 */
function cancelConnectionFlow(resetRetryAttempt = true): void {
	nextConnectionSessionId();
	isConnecting = false;
	clearRetryTimer();
	stopHeartbeat();
	handshakeVerified = false;
	currentPort = null;
	windowId = null;
	if (resetRetryAttempt) {
		retryAttempt = 0;
	}
	closeWebSocket();
}

function performReconnect(): void {
	connectionIntent = 'manual';
	eda.sys_Message.showToastMessage(eda.sys_I18n.text('Reconnecting...'));
	cancelConnectionFlow();
	setConnectionState('scanning', 'manual reconnect');
	void scanAndConnect();
}

function performStopConnection(showToast = true): void {
	connectionIntent = 'manual-stopped';
	cancelConnectionFlow();
	setConnectionState('manual-stopped', 'manual stop');
	if (showToast) {
		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Connection stopped'));
	}
}

async function dispatchControlCommand(command: GatewayControlRequest['command']): Promise<void> {
	try {
		const response = await eda.sys_MessageBus.rpcCall(MBUS_TOPIC_CONTROL, { command }, 500) as GatewayControlResponse;
		if (response?.handled) {
			if (command === 'stop') {
				eda.sys_Message.showToastMessage(eda.sys_I18n.text('Connection stopped'));
			}
			return;
		}
	}
	catch {
		// 当前窗口未注册 MessageBus 服务时，回退到本窗口直接处理。
	}

	ensureMessageBusServices();
	if (command === 'reconnect') {
		performReconnect();
	}
	else {
		performStopConnection();
	}
}

// ─── 生命周期 ────────────────────────────────────────────────────────

/**
 * 扩展激活入口（支持 onStartupFinished 自动启动）。
 */
// eslint-disable-next-line unused-imports/no-unused-vars
export function activate(status?: 'onStartupFinished', arg?: string): void {
	ensureMessageBusServices();
	const storedValue = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT);
	autoConnectEnabled = storedValue !== false;
	connectionIntent = autoConnectEnabled ? 'auto' : 'disabled';

	if (shouldMaintainConnection()) {
		setConnectionState('scanning', 'extension activated');
		void scanAndConnect();
	}
	else {
		setConnectionState('disabled', 'auto-connect disabled');
	}
}

/**
 * 扩展停用时清理资源，不再发起新连接。
 */
export function deactivate(): void {
	connectionIntent = 'disabled';
	cancelConnectionFlow(false);
	setConnectionState('disabled', 'extension deactivated');
}

// ─── 菜单操作 ────────────────────────────────────────────────────────

/**
 * 手动重新连接。该操作会建立 manual 连接意图；即使自动连接关闭，
 * 连接中断后仍会继续恢复，直到用户点击“停止连接”或关闭扩展。
 */
export function reconnect(): void {
	void dispatchControlCommand('reconnect');
}

/**
 * 关于对话框，同时显示状态机、连接意图和下次重试信息。
 */
export async function about(): Promise<void> {
	let statusInfo: GatewayConnectionStatus = getConnectionStatus();
	try {
		statusInfo = await eda.sys_MessageBus.rpcCall(MBUS_TOPIC_STATUS, undefined, 300) as GatewayConnectionStatus;
	}
	catch {
		// MessageBus 不可用时显示当前窗口本地状态。
	}

	const connectionLine = statusInfo.connected
		? `Connected (port ${statusInfo.port})`
		: statusInfo.connecting
			? 'Connecting...'
			: 'Disconnected';
	const windowLine = statusInfo.windowId ?? '(not registered)';
	const retryLine = statusInfo.nextRetryAt
		? `${statusInfo.retryAttempt} / ${Math.max(0, Math.ceil((statusInfo.nextRetryAt - Date.now()) / 1000))}s`
		: String(statusInfo.retryAttempt);

	eda.sys_Dialog.showInformationMessage(
		[
			`API Gateway v${extensionConfig.version}`,
			connectionLine,
			`State: ${statusInfo.state}`,
			`Intent: ${statusInfo.intent}`,
			`Auto-Connect: ${statusInfo.autoConnectEnabled ? 'enabled' : 'disabled'}`,
			`Retry: ${retryLine}`,
			`Window ID: ${windowLine}`,
			statusInfo.lastError ? `Last error: ${statusInfo.lastError}` : '',
		].filter(Boolean).join('\n'),
		'About',
	);
}

/**
 * 切换自动连接开关，并立即影响当前运行状态。
 *
 * - 启用：立即开始或接管为 auto 连接意图；
 * - 禁用：立即停止当前连接、扫描、心跳和重试；
 * - 禁用后仍可通过“重新连接”建立 manual 连接。
 */
export async function toggleAutoConnect(): Promise<void> {
	const storedValue = eda.sys_Storage.getExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT);
	const currentlyEnabled = storedValue !== false;
	const nextEnabled = !currentlyEnabled;
	await eda.sys_Storage.setExtensionUserConfig(STORAGE_KEY_AUTO_CONNECT, nextEnabled);
	autoConnectEnabled = nextEnabled;

	if (nextEnabled) {
		connectionIntent = 'auto';
		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Auto-Connect enabled'));
		if (!handshakeVerified && !isConnecting) {
			cancelConnectionFlow();
			setConnectionState('scanning', 'auto-connect enabled');
			void scanAndConnect();
		}
	}
	else {
		connectionIntent = 'disabled';
		cancelConnectionFlow();
		setConnectionState('disabled', 'auto-connect disabled');
		eda.sys_Message.showToastMessage(eda.sys_I18n.text('Auto-Connect disabled'));
	}
}

/**
 * 停止当前连接并取消本次会话的自动恢复。
 * 持久化的自动连接设置不变，下次扩展重新加载时仍按该设置启动。
 */
export function stopConnection(): void {
	void dispatchControlCommand('stop');
}

// ─── 端口扫描与连接 ──────────────────────────────────────────────────

/**
 * 扫描端口范围，通过 WebSocket 握手识别 Bridge。
 *
 * 与旧实现不同，本函数不设置最大重试次数。Bridge 未启动或重启时，
 * Gateway 会使用带抖动的封顶指数退避持续等待，直到连接意图被关闭。
 */
async function scanAndConnect(): Promise<void> {
	if (!shouldMaintainConnection()) {
		setConnectionState(getIdleStateForIntent(), 'connection intent inactive');
		return;
	}
	if (isConnecting || handshakeVerified) {
		return;
	}

	const sessionId = nextConnectionSessionId();
	isConnecting = true;
	clearRetryTimer();
	setConnectionState('scanning', `ports ${PORT_START}-${PORT_END}`);

	try {
		for (let port = PORT_START; port <= PORT_END; port++) {
			if (!isConnectionSessionActive(sessionId) || !shouldMaintainConnection()) {
				return;
			}

			setConnectionState('connecting', `port ${port}`);
			const found = await tryConnectToPort(port, sessionId);
			if (!isConnectionSessionActive(sessionId) || !shouldMaintainConnection()) {
				return;
			}

			if (found) {
				currentPort = port;
				retryAttempt = 0;
				nextRetryAt = null;
				lastError = null;
				lastConnectedAt = Date.now();
				setConnectionState('connected', `port ${port}`);
				startHeartbeat(sessionId);
				eda.sys_Message.showToastMessage(
					eda.sys_I18n.text('Bridge connected', undefined, undefined, String(port)),
				);
				return;
			}
		}

		retryAttempt += 1;
		lastError = `Bridge not found on ports ${PORT_START}-${PORT_END}`;
		const retryDelayMs = calculateRetryDelay(retryAttempt);
		setConnectionState('waiting-bridge', lastError);
		showRateLimitedToast(
			'bridge-waiting',
			eda.sys_I18n.text(
				'Bridge not found; retrying',
				undefined,
				undefined,
				String(Math.ceil(retryDelayMs / 1000)),
				String(retryAttempt),
			),
		);
		scheduleRetry(sessionId, retryDelayMs);
	}
	catch (err: unknown) {
		lastError = err instanceof Error ? err.message : String(err);
		console.error('[API-Gateway] Connection scan failed:', lastError);
		setConnectionState('error', lastError);
		if (shouldMaintainConnection() && isConnectionSessionActive(sessionId)) {
			retryAttempt += 1;
			scheduleRetry(sessionId, calculateRetryDelay(retryAttempt));
		}
	}
	finally {
		if (isConnectionSessionActive(sessionId)) {
			isConnecting = false;
		}
	}
}

/**
 * 尝试连接单个端口并等待服务端 handshake。
 * 旧 session 的迟到回调只能结束旧 Promise，不得关闭新的 WS 连接。
 */
function tryConnectToPort(port: number, sessionId: number): Promise<boolean> {
	return new Promise((resolve) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | null = null;

		const settle = (success: boolean, reason: string): void => {
			if (settled) {
				return;
			}
			settled = true;
			if (timer) {
				clearTimeout(timer);
				timer = null;
			}
			if (!success && isConnectionSessionActive(sessionId)) {
				closeWebSocket();
			}
			if (!success) {
				console.debug(`[API-Gateway] Port ${port} rejected: ${reason}`);
			}
			resolve(success);
		};

		if (!isConnectionSessionActive(sessionId)) {
			resolve(false);
			return;
		}

		// register 使用固定 WS_ID；重新注册前必须先关闭旧连接。
		closeWebSocket();
		timer = setTimeout(() => settle(false, 'handshake timeout'), CONNECTION_TIMEOUT_MS);
		handshakeVerified = false;

		try {
			eda.sys_WebSocket.register(
				WS_ID,
				`ws://127.0.0.1:${port}/eda`,
				async (event: MessageEvent) => {
					if (!isConnectionSessionActive(sessionId)) {
						settle(false, 'session cancelled');
						return;
					}

					try {
						const msg = JSON.parse(String(event.data)) as BridgeMessage;
						if (msg.type === 'handshake') {
							if (msg.service !== SERVICE_ID) {
								settle(false, `unexpected service: ${String(msg.service)}`);
								return;
							}

							handshakeVerified = true;
							windowId = crypto.randomUUID();
							eda.sys_WebSocket.send(WS_ID, JSON.stringify({
								type: 'register',
								windowId,
								timestamp: Date.now(),
							}));
							settle(true, 'handshake verified');
							return;
						}

						if (!handshakeVerified) {
							return;
						}
						await handleMessage(msg);
					}
					catch (err: unknown) {
						console.error(
							'[API-Gateway] Failed to handle message:',
							err instanceof Error ? err.message : String(err),
						);
					}
				},
				() => {
					// TCP/WS 已建立；Bridge 身份仍必须由后续 handshake 验证。
				},
			);
		}
		catch (err: unknown) {
			const errorMessage = err instanceof Error ? err.message : String(err);
			console.error('[API-Gateway] Failed to register WebSocket:', errorMessage);
			settle(false, `register failed: ${errorMessage}`);
		}
	});
}

// ─── 心跳检测 ────────────────────────────────────────────────────────

function handleConnectionLost(reason: string, sessionId: number): void {
	if (!isConnectionSessionActive(sessionId)) {
		return;
	}

	lastError = reason;
	console.warn(`[API-Gateway] ${reason}`);
	cancelConnectionFlow();
	if (shouldMaintainConnection()) {
		setConnectionState('scanning', reason);
		void scanAndConnect();
	}
	else {
		setConnectionState(getIdleStateForIntent(), reason);
	}
}

function startHeartbeat(sessionId: number): void {
	stopHeartbeat();
	heartbeatTimer = setInterval(() => {
		if (!isConnectionSessionActive(sessionId)) {
			stopHeartbeat();
			return;
		}
		if (!handshakeVerified || heartbeatPending) {
			return;
		}

		try {
			heartbeatPending = true;
			eda.sys_WebSocket.send(WS_ID, JSON.stringify({
				type: 'ping',
				id: `hb-${Date.now()}`,
				timestamp: Date.now(),
			}));
			clearHeartbeatTimeout();
			heartbeatTimeoutTimer = setTimeout(() => {
				if (isConnectionSessionActive(sessionId) && heartbeatPending) {
					handleConnectionLost('Heartbeat timeout; reconnecting', sessionId);
				}
			}, HEARTBEAT_TIMEOUT_MS);
		}
		catch (err: unknown) {
			const errorMessage = err instanceof Error ? err.message : String(err);
			handleConnectionLost(`Heartbeat send failed: ${errorMessage}`, sessionId);
		}
	}, HEARTBEAT_INTERVAL_MS);
}

// ─── 重试 ────────────────────────────────────────────────────────────

/**
 * 计算带 ±20% 抖动的封顶指数退避。
 * 指数在达到 30 秒上限后停止增长，避免长时间离线后出现溢出或极端等待。
 */
function calculateRetryDelay(attempt: number): number {
	const exponent = Math.min(Math.max(attempt - 1, 0), 5);
	const baseDelay = Math.min(INITIAL_RETRY_DELAY_MS * 2 ** exponent, MAX_RETRY_DELAY_MS);
	const jitterSpan = Math.floor(baseDelay * RETRY_JITTER_RATIO);
	const jitter = Math.floor(Math.random() * (jitterSpan * 2 + 1)) - jitterSpan;
	return Math.max(500, baseDelay + jitter);
}

function scheduleRetry(sessionId: number, delayMs: number): void {
	clearRetryTimer();
	if (!shouldMaintainConnection() || !isConnectionSessionActive(sessionId)) {
		setConnectionState(getIdleStateForIntent(), 'retry cancelled');
		return;
	}

	nextRetryAt = Date.now() + delayMs;
	setConnectionState('backoff', `${delayMs}ms`);
	retryTimer = setTimeout(() => {
		retryTimer = null;
		nextRetryAt = null;
		if (!isConnectionSessionActive(sessionId) || !shouldMaintainConnection()) {
			return;
		}
		void scanAndConnect();
	}, delayMs);
}

// ─── 消息处理 ────────────────────────────────────────────────────────

async function handleMessage(msg: BridgeMessage): Promise<void> {
	if (msg.type === 'ping') {
		eda.sys_WebSocket.send(WS_ID, JSON.stringify({
			type: 'pong',
			id: msg.id,
			timestamp: Date.now(),
		}));
		return;
	}

	if (msg.type === 'pong') {
		heartbeatPending = false;
		clearHeartbeatTimeout();
		return;
	}

	if (msg.type === 'execute' && msg.code) {
		try {
			// AsyncFunction 允许 Bridge 代码直接使用 await 调用 EasyEDA API。
			const AsyncFunction = Object.getPrototypeOf(async () => {}).constructor;
			const fn = new AsyncFunction('eda', msg.code);
			const result = await fn(eda);

			eda.sys_WebSocket.send(WS_ID, JSON.stringify({
				type: 'result',
				id: msg.id,
				result: result !== undefined ? result : null,
				timestamp: Date.now(),
			}));
		}
		catch (err: unknown) {
			eda.sys_WebSocket.send(WS_ID, JSON.stringify({
				type: 'error',
				id: msg.id,
				error: err instanceof Error ? err.message : String(err),
				timestamp: Date.now(),
			}));
		}
	}
}
