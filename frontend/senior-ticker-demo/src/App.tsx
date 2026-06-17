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
  config: DemoConfig;
  timestamp: string;
  modes: ModeSnapshot[];
};

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
  config: defaultConfig,
  timestamp: new Date(0).toISOString(),
  modes: [],
};

const formatNumber = new Intl.NumberFormat("ru-RU");

export default function App() {
  const [snapshot, setSnapshot] = useState<DemoSnapshot>(emptySnapshot);
  const [config, setConfig] = useState<DemoConfig>(defaultConfig);
  const [status, setStatus] = useState("подключение");
  const [busy, setBusy] = useState(false);
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
        if (active) setStatus("api недоступен");
      });

    const events = new EventSource("/api/demo/events");
    events.onopen = () => setStatus("live");
    events.onerror = () => setStatus("переподключение");
    events.onmessage = (event) => applySnapshot(JSON.parse(event.data) as DemoSnapshot);

    return () => {
      active = false;
      events.close();
    };
  }, [applySnapshot]);

  const start = async () => {
    setBusy(true);
    try {
      const next = await postSnapshot("/api/demo/start", config);
      applySnapshot(next);
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    setBusy(true);
    try {
      const next = await postSnapshot("/api/demo/stop");
      applySnapshot(next);
    } finally {
      setBusy(false);
    }
  };

  const reset = async () => {
    setBusy(true);
    try {
      previous.current = null;
      setRates({});
      const next = await postSnapshot("/api/demo/reset");
      applySnapshot(next);
    } finally {
      setBusy(false);
    }
  };

  const totalWritten = useMemo(
    () => snapshot.modes.reduce((sum, mode) => sum + mode.written, 0),
    [snapshot.modes],
  );

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">SeniorTicker</p>
          <h1>Сравнение обработки hot symbol</h1>
        </div>
        <div className="status-cluster" aria-label="Состояние демо">
          <span className={`status-dot ${snapshot.running ? "is-running" : ""}`} />
          <span>{snapshot.running ? "running" : "stopped"}</span>
          <span className="muted">{status}</span>
        </div>
      </header>

      <section className="control-band" aria-label="Параметры нагрузки">
        <div className="controls-grid">
          <Control
            label="Тиков в секунду"
            value={config.ratePerSecond}
            min={10}
            max={50_000}
            step={10}
            onChange={(ratePerSecond) => setConfig((current) => ({ ...current, ratePerSecond }))}
          />
          <Control
            label="Доля BTCUSDT"
            value={config.hotSymbolPercent}
            min={0}
            max={100}
            step={1}
            suffix="%"
            onChange={(hotSymbolPercent) => setConfig((current) => ({ ...current, hotSymbolPercent }))}
          />
          <Control
            label="Дубли"
            value={config.duplicatePercent}
            min={0}
            max={90}
            step={1}
            suffix="%"
            onChange={(duplicatePercent) => setConfig((current) => ({ ...current, duplicatePercent }))}
          />
          <Control
            label="Шарды"
            value={config.shardCount}
            min={1}
            max={32}
            step={1}
            onChange={(shardCount) => setConfig((current) => ({ ...current, shardCount }))}
          />
          <Control
            label="Writers"
            value={config.writerCount}
            min={1}
            max={16}
            step={1}
            onChange={(writerCount) => setConfig((current) => ({ ...current, writerCount }))}
          />
          <Control
            label="Размер batch"
            value={config.batchMaxSize}
            min={1}
            max={900}
            step={1}
            onChange={(batchMaxSize) => setConfig((current) => ({ ...current, batchMaxSize }))}
          />
          <Control
            label="Задержка БД"
            value={config.sinkDelayMs}
            min={0}
            max={250}
            step={5}
            suffix=" ms"
            onChange={(sinkDelayMs) => setConfig((current) => ({ ...current, sinkDelayMs }))}
          />
        </div>

        <div className="command-cluster">
          <button type="button" className="primary" onClick={start} disabled={busy}>
            {snapshot.running ? "Применить и перезапустить" : "Запустить"}
          </button>
          <button type="button" onClick={stop} disabled={busy || !snapshot.running}>
            Остановить
          </button>
          <button type="button" onClick={reset} disabled={busy}>
            Очистить БД
          </button>
        </div>
      </section>

      <section className="summary-strip" aria-label="Сводка">
        <Metric label="Конфиг" value={`${snapshot.config.shardCount} shards / ${snapshot.config.writerCount} writers`} />
        <Metric label="Вход" value={`${formatNumber.format(snapshot.config.ratePerSecond)} ticks/s`} />
        <Metric label="Записано всего" value={formatNumber.format(totalWritten)} />
      </section>

      <section className="modes-grid" aria-label="Режимы обработки">
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
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  suffix?: string;
  onChange: (value: number) => void;
}) {
  return (
    <label className="control">
      <span className="control-label">
        <span>{props.label}</span>
        <strong>
          {formatNumber.format(props.value)}
          {props.suffix ?? ""}
        </strong>
      </span>
      <input
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
          <p className="eyebrow">{mode.mode}</p>
          <h2>{mode.name}</h2>
        </div>
        <span className={`mode-state ${mode.running ? "active" : ""}`}>{mode.running ? "live" : "idle"}</span>
      </header>

      <div className="pipeline-row" aria-label={`Pipeline ${mode.name}`}>
        <PipelineStep label="Ingest" value={mode.ingestDepth} tone="yellow" />
        <PipelineStep label="Router" value={`${Math.round(skew * 100)}%`} tone={skew > 0.7 ? "red" : "green"} />
        <PipelineStep label="Shards" value={mode.shardDepths.length} tone="green" />
        <PipelineStep label="Batch" value={mode.batchDepth} tone={mode.batchDepth > 0 ? "yellow" : "green"} />
        <PipelineStep label="Postgres" value={formatNumber.format(mode.dbRows)} tone="purple" />
      </div>

      <div className="numbers-grid">
        <Metric label="Accepted" value={formatNumber.format(mode.accepted)} />
        <Metric label="Received/s" value={formatNumber.format(Math.round(rates?.receivedPerSecond ?? 0))} />
        <Metric label="Written/s" value={formatNumber.format(Math.round(rates?.writtenPerSecond ?? 0))} />
        <Metric label="Duplicates" value={formatNumber.format(mode.deduplicated)} />
        <Metric label="Pending unique" value={formatNumber.format(uniquePending)} />
        <Metric label="Pressure" value={formatNumber.format(pressure)} />
      </div>

      <ShardChart title="Распределение входа по shard" values={mode.shardAccepted} max={acceptedMax} />
      <ShardChart title="Глубина очередей shard" values={mode.shardDepths} max={depthMax} compact />
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
  if (value >= 1_000_000) return `${Math.round(value / 100_000) / 10}m`;
  if (value >= 1_000) return `${Math.round(value / 100) / 10}k`;
  return String(value);
}
