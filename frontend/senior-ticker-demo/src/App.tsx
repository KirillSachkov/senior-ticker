import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type DemoConfig = {
  ratePerSecond: number;
  hotSymbolPercent: number;
  duplicatePercent: number;
  shardCount: number;
  writerCount: number;
  batchMaxSize: number;
  sinkDelayMs: number;
};

type ModeSnapshot = {
  mode: string;
  name: string;
  running: boolean;
  accepted: number;
  received: number;
  deduplicated: number;
  written: number;
  dbRows: number;
  ingestDepth: number;
  batchDepth: number;
  shardDepths: number[];
  shardAccepted: number[];
};

type DemoSnapshot = {
  running: boolean;
  state: string;
  config: DemoConfig;
  timestamp: string;
  modes: ModeSnapshot[];
};

type BusyAction = "start" | "stop" | "reset";

type ModeRates = {
  receivedPerSecond: number;
  writtenPerSecond: number;
};

const defaultConfig: DemoConfig = {
  ratePerSecond: 1_000,
  hotSymbolPercent: 95,
  duplicatePercent: 10,
  shardCount: 8,
  writerCount: 2,
  batchMaxSize: 500,
  sinkDelayMs: 0,
};

const emptySnapshot: DemoSnapshot = {
  running: false,
  state: "stopped",
  config: defaultConfig,
  timestamp: new Date(0).toISOString(),
  modes: [],
};

const formatNumber = new Intl.NumberFormat("ru-RU");

export default function App() {
  const [snapshot, setSnapshot] = useState<DemoSnapshot>(emptySnapshot);
  const [config, setConfig] = useState<DemoConfig>(defaultConfig);
  const [connectionStatus, setConnectionStatus] = useState("подключение");
  const [busyAction, setBusyAction] = useState<BusyAction | null>(null);
  const [rates, setRates] = useState<Record<string, ModeRates>>({});
  const previous = useRef<DemoSnapshot | null>(null);

  const applySnapshot = useCallback((next: DemoSnapshot) => {
    const prev = previous.current;
    if (prev) {
      const seconds = Math.max(0.001, (Date.parse(next.timestamp) - Date.parse(prev.timestamp)) / 1_000);
      const nextRates: Record<string, ModeRates> = {};
      for (const mode of next.modes) {
        const old = prev.modes.find((item) => item.mode === mode.mode);
        nextRates[mode.mode] = {
          receivedPerSecond: old ? Math.max(0, (mode.received - old.received) / seconds) : 0,
          writtenPerSecond: old ? Math.max(0, (mode.written - old.written) / seconds) : 0,
        };
      }
      setRates(nextRates);
    }
    previous.current = next;
    setSnapshot(next);
  }, []);

  useEffect(() => {
    let active = true;

    fetch("/api/demo/snapshot")
      .then((response) => response.json())
      .then((data: DemoSnapshot) => {
        if (!active) return;
        applySnapshot(data);
        setConfig(data.config);
      })
      .catch(() => {
        if (active) setConnectionStatus("api недоступен");
      });

    const events = new EventSource("/api/demo/events");
    events.onopen = () => setConnectionStatus("подключено");
    events.onerror = () => setConnectionStatus("переподключение");
    events.onmessage = (event) => applySnapshot(JSON.parse(event.data) as DemoSnapshot);

    return () => {
      active = false;
      events.close();
    };
  }, [applySnapshot]);

  const runAction = async (action: BusyAction, request: () => Promise<DemoSnapshot>) => {
    setBusyAction(action);
    try {
      const next = await request();
      applySnapshot(next);
    } catch {
      setConnectionStatus("ошибка запроса");
    } finally {
      setBusyAction(null);
    }
  };

  const start = async () => runAction("start", () => postSnapshot("/api/demo/start", config));

  const stop = async () => runAction("stop", () => postSnapshot("/api/demo/stop"));

  const reset = async () => {
    previous.current = null;
    setRates({});
    await runAction("reset", () => postSnapshot("/api/demo/reset"));
  };

  const totalWritten = useMemo(
    () => snapshot.modes.reduce((sum, mode) => sum + mode.written, 0),
    [snapshot.modes],
  );
  const state = busyAction ?? snapshot.state;
  const commandBusy = busyAction !== null || isTransitionState(snapshot.state);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">SeniorTicker</p>
          <h1>Конвейер обработки горячего символа под нагрузкой</h1>
        </div>
        <div className="status-cluster" aria-label="Состояние демо">
          <span className={`status-dot ${snapshot.running ? "is-running" : ""} ${isTransitionState(state) ? "is-busy" : ""}`} />
          <span>{stateLabel(state)}</span>
          <span className="muted">{connectionStatus}</span>
        </div>
      </header>

      <form
        className="control-band"
        aria-label="Параметры нагрузки"
        onSubmit={(event) => {
          event.preventDefault();
          void start();
        }}
      >
        <div className="controls-grid">
          <Control
            name="rate-per-second"
            label="Тиков в секунду"
            value={config.ratePerSecond}
            min={10}
            max={50_000}
            step={10}
            onChange={(ratePerSecond) => setConfig((current) => ({ ...current, ratePerSecond }))}
          />
          <Control
            name="hot-symbol-percent"
            label="Доля BTCUSDT"
            value={config.hotSymbolPercent}
            min={0}
            max={100}
            step={1}
            suffix="%"
            onChange={(hotSymbolPercent) => setConfig((current) => ({ ...current, hotSymbolPercent }))}
          />
          <Control
            name="duplicate-percent"
            label="Дубли"
            value={config.duplicatePercent}
            min={0}
            max={90}
            step={1}
            suffix="%"
            onChange={(duplicatePercent) => setConfig((current) => ({ ...current, duplicatePercent }))}
          />
          <Control
            name="shard-count"
            label="Шарды"
            value={config.shardCount}
            min={1}
            max={32}
            step={1}
            onChange={(shardCount) => setConfig((current) => ({ ...current, shardCount }))}
          />
          <Control
            name="writer-count"
            label="Писатели в БД"
            value={config.writerCount}
            min={1}
            max={16}
            step={1}
            onChange={(writerCount) => setConfig((current) => ({ ...current, writerCount }))}
          />
          <Control
            name="batch-max-size"
            label="Размер батча"
            value={config.batchMaxSize}
            min={1}
            max={900}
            step={1}
            onChange={(batchMaxSize) => setConfig((current) => ({ ...current, batchMaxSize }))}
          />
          <Control
            name="sink-delay-ms"
            label="Задержка БД"
            value={config.sinkDelayMs}
            min={0}
            max={250}
            step={5}
            suffix=" мс"
            onChange={(sinkDelayMs) => setConfig((current) => ({ ...current, sinkDelayMs }))}
          />
        </div>

        <div className="command-cluster">
          <button type="submit" className="primary" disabled={commandBusy}>
            {startButtonText(busyAction, snapshot.running)}
          </button>
          <button type="button" onClick={stop} disabled={commandBusy || snapshot.state !== "running"}>
            {busyAction === "stop" ? "Останавливаю..." : "Остановить"}
          </button>
          <button type="button" onClick={reset} disabled={commandBusy}>
            {busyAction === "reset" ? "Очищаю..." : "Очистить БД"}
          </button>
        </div>
      </form>

      <section className="summary-strip" aria-label="Сводка">
        <Metric label="Конфиг" value={`${snapshot.config.shardCount} шардов / ${snapshot.config.writerCount} писателей`} />
        <Metric label="Вход" value={`${formatNumber.format(snapshot.config.ratePerSecond)} тиков/с`} />
        <Metric label="Записано всего" value={formatNumber.format(totalWritten)} />
      </section>

      <section className="modes-grid" aria-label="Конвейер обработки">
        {snapshot.modes.map((mode) => (
          <ModeCard key={mode.mode} mode={mode} rates={rates[mode.mode]} />
        ))}
        {snapshot.modes.length === 0 && (
          <div className="empty-state">Нет снимка API. Проверь, что demo-api запущен.</div>
        )}
      </section>
    </main>
  );
}

async function postSnapshot(url: string, body?: DemoConfig): Promise<DemoSnapshot> {
  const response = await fetch(url, {
    method: "POST",
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) throw new Error(`${url}: ${response.status}`);
  return (await response.json()) as DemoSnapshot;
}

function Control(props: {
  name: string;
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  suffix?: string;
  onChange: (value: number) => void;
}) {
  const id = `control-${props.name}`;

  return (
    <label className="control" htmlFor={id}>
      <span className="control-label">
        <span>{props.label}</span>
        <strong>
          {formatNumber.format(props.value)}
          {props.suffix ?? ""}
        </strong>
      </span>
      <input
        id={id}
        name={props.name}
        type="range"
        min={props.min}
        max={props.max}
        step={props.step}
        value={props.value}
        onChange={(event) => props.onChange(Number(event.target.value))}
      />
    </label>
  );
}

function Metric(props: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function ModeCard({ mode, rates }: { mode: ModeSnapshot; rates?: ModeRates }) {
  const acceptedMax = Math.max(1, ...mode.shardAccepted);
  const depthMax = Math.max(1, ...mode.shardDepths);
  const uniquePending = Math.max(0, mode.received - mode.deduplicated - mode.written);
  const hottestShard = mode.shardAccepted.length === 0 ? 0 : Math.max(...mode.shardAccepted);
  const skew = mode.accepted === 0 ? 0 : hottestShard / mode.accepted;
  const pressure = mode.ingestDepth + mode.batchDepth + mode.shardDepths.reduce((sum, depth) => sum + depth, 0);

  return (
    <article className="mode-card">
      <header className="mode-header">
        <div>
          <p className="eyebrow">{modeCaption(mode.mode)}</p>
          <h2>{mode.name}</h2>
        </div>
        <span className={`mode-state ${mode.running ? "active" : ""}`}>{mode.running ? "работает" : "стоит"}</span>
      </header>

      <div className="pipeline-row" aria-label={`Пайплайн ${mode.name}`}>
        <PipelineStep label="Вход" value={mode.ingestDepth} tone="yellow" />
        <PipelineStep label="Роутер" value={`${Math.round(skew * 100)}%`} tone={skew > 0.7 ? "red" : "green"} />
        <PipelineStep label="Шарды" value={mode.shardDepths.length} tone="green" />
        <PipelineStep label="Батчи" value={mode.batchDepth} tone={mode.batchDepth > 0 ? "yellow" : "green"} />
        <PipelineStep label="PostgreSQL" value={formatNumber.format(mode.dbRows)} tone="purple" />
      </div>

      <div className="numbers-grid">
        <Metric label="Принято" value={formatNumber.format(mode.accepted)} />
        <Metric label="Получено/с" value={formatNumber.format(Math.round(rates?.receivedPerSecond ?? 0))} />
        <Metric label="Записано/с" value={formatNumber.format(Math.round(rates?.writtenPerSecond ?? 0))} />
        <Metric label="Пропущено дублей" value={formatNumber.format(mode.deduplicated)} />
        <Metric label="Ожидают записи" value={formatNumber.format(uniquePending)} />
        <Metric label="Очереди всего" value={formatNumber.format(pressure)} />
      </div>

      <ShardChart title="Распределение входа по шардам" values={mode.shardAccepted} max={acceptedMax} />
      <ShardChart title="Глубина очередей шардов" values={mode.shardDepths} max={depthMax} compact />
    </article>
  );
}

function PipelineStep(props: { label: string; value: string | number; tone: "green" | "yellow" | "red" | "purple" }) {
  return (
    <div className={`pipeline-step ${props.tone}`}>
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function ShardChart(props: { title: string; values: number[]; max: number; compact?: boolean }) {
  return (
    <div className={props.compact ? "shard-chart compact" : "shard-chart"}>
      <div className="chart-title">{props.title}</div>
      <div className="bar-row">
        {props.values.map((value, index) => {
          const height = Math.max(4, Math.round((value / props.max) * 100));
          return (
            <div className="bar-cell" key={index}>
              <span className="bar-value">{props.compact ? value : shortNumber(value)}</span>
              <span className="bar-track">
                <span className="bar-fill" style={{ height: `${height}%` }} />
              </span>
              <span className="bar-label">{index}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function shortNumber(value: number) {
  if (value >= 1_000_000) return `${Math.round(value / 100_000) / 10} млн`;
  if (value >= 1_000) return `${Math.round(value / 100) / 10} тыс`;
  return String(value);
}

function modeCaption(mode: string) {
  switch (mode) {
    case "channels-dedup-key":
      return "Channels + шарды по ключу тика";
    default:
      return mode;
  }
}

function isTransitionState(state: string) {
  return state === "start"
    || state === "stop"
    || state === "reset"
    || state === "starting"
    || state === "stopping"
    || state === "resetting";
}

function stateLabel(state: string) {
  switch (state) {
    case "start":
    case "starting":
      return "запускается";
    case "running":
      return "работает";
    case "stop":
    case "stopping":
      return "останавливается";
    case "reset":
    case "resetting":
      return "очищается БД";
    case "stopped":
      return "остановлено";
    default:
      return state;
  }
}

function startButtonText(action: BusyAction | null, running: boolean) {
  if (action === "start") return running ? "Перезапускаю..." : "Запускаю...";
  if (action === "stop") return "Дренаж...";
  if (action === "reset") return "Очищаю...";
  return running ? "Применить и перезапустить" : "Запустить";
}
