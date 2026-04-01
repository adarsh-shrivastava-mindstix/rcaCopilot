const API_URL = "http://127.0.0.1:8080/invocations";

const form = document.getElementById("rca-form");
const logIdInput = document.getElementById("logId");
const submitBtn = document.getElementById("submitBtn");
const statusBox = document.getElementById("status");
const progressSection = document.getElementById("progressSection");
const stepTimeline = document.getElementById("stepTimeline");
const reportSection = document.getElementById("reportSection");

const reportBadge = document.getElementById("reportBadge");
const reportId = document.getElementById("reportId");
const outLogId = document.getElementById("outLogId");
const service = document.getElementById("service");
const confidence = document.getElementById("confidence");
const directory = document.getElementById("directory");
const filePath = document.getElementById("filePath");
const functionName = document.getElementById("functionName");
const lineNo = document.getElementById("lineNo");
const issueSummary = document.getElementById("issueSummary");
const rootCause = document.getElementById("rootCause");
const webSolution = document.getElementById("webSolution");
const agentSolution = document.getElementById("agentSolution");
const combinedSolution = document.getElementById("combinedSolution");
const nextActions = document.getElementById("nextActions");
const preventiveActions = document.getElementById("preventiveActions");
const webReferences = document.getElementById("webReferences");
const markdownOutput = document.getElementById("markdownOutput");

const STEP_TITLES = [
  "Validating Log ID & Fetching Logs From Database",
  "Analyzing Logs and Detecting Error",
  "Locating Source File and Function",
  "Fetching GitHub Context & Researching Fixes on Tavily",
  "Bedrock Reasoning & Drafting Final RCA",
  "Storing Report to S3",
  "RCA Report Ready",
];

const STEP_KEYS = [
  "validate_fetch",
  "analyze_logs",
  "locate_source",
  "github_tavily",
  "bedrock_draft",
  "store_s3",
  "report_ready",
];

const STEP_KEY_TO_INDEX = STEP_KEYS.reduce((acc, key, idx) => {
  acc[key] = idx;
  return acc;
}, {});

const STEP_META = {
  pending: "Pending",
  loading: "In progress...",
  completed: "Completed",
  failed: "Failed",
  skipped: "Skipped",
};

let stepStates = [];
let stepTiming = [];
let stepStreams = [];
let latestReport = null;
let latestStorage = null;

function setStatus(message, kind) {
  statusBox.className = `status ${kind}`;
  statusBox.textContent = message;
  statusBox.classList.remove("hidden");
}

function ensureProgressVisible() {
  if (!progressSection) return;
  progressSection.classList.remove("hidden");
  progressSection.style.display = "block";
}

function formatDurationMs(ms) {
  if (!Number.isFinite(ms) || ms < 0) return "";
  return `${(ms / 1000).toFixed(2)}s`;
}

function deriveDurationMs(index, providedDurationMs) {
  if (Number.isFinite(providedDurationMs)) {
    return providedDurationMs;
  }
  const timing = stepTiming[index];
  if (!timing || !timing.startedAt || !timing.endedAt) {
    return null;
  }
  return timing.endedAt - timing.startedAt;
}

function buildMetaText(index, status, message, providedDurationMs) {
  const base = STEP_META[status] || STEP_META.pending;
  const durationMs = deriveDurationMs(index, providedDurationMs);
  const durationText = formatDurationMs(durationMs);

  if (status === "loading") {
    return base;
  }

  if (status === "failed" && message) {
    return durationText ? `${base} - ${durationText} - ${message}` : `${base} - ${message}`;
  }

  if (durationText) {
    return `${base} - ${durationText}`;
  }

  if (message) {
    return `${base} - ${message}`;
  }

  return base;
}

function renderTimeline() {
  if (!stepTimeline) return;
  stepTimeline.innerHTML = "";

  STEP_TITLES.forEach((title, index) => {
    const li = document.createElement("li");
    li.className = "step-item pending";
    li.dataset.index = String(index);

    const icon = document.createElement("span");
    icon.className = "step-icon";

    const content = document.createElement("div");
    const titleNode = document.createElement("p");
    titleNode.className = "step-title";
    titleNode.textContent = title;

    const metaNode = document.createElement("p");
    metaNode.className = "step-meta";
    metaNode.textContent = STEP_META.pending;

    const streamNode = document.createElement("p");
    streamNode.className = "step-stream";
    streamNode.textContent = "";

    content.appendChild(titleNode);
    content.appendChild(metaNode);
    content.appendChild(streamNode);
    li.appendChild(icon);
    li.appendChild(content);
    stepTimeline.appendChild(li);
  });
}

function setStepStatus(index, status, options = {}) {
  if (!stepTimeline) return;
  if (index < 0 || index >= STEP_TITLES.length) return;

  const { message = "", durationMs = null, startedAt = null, endedAt = null } = options;
  const prev = stepStates[index];
  const now = Date.now();

  if (!stepTiming[index]) {
    stepTiming[index] = { startedAt: null, endedAt: null };
  }

  if (status === "loading") {
    stepTiming[index].startedAt = startedAt ? new Date(startedAt).getTime() : now;
    stepTiming[index].endedAt = null;
  } else {
    if (!stepTiming[index].startedAt) {
      stepTiming[index].startedAt = startedAt ? new Date(startedAt).getTime() : now;
    }
    if (prev === "loading" || stepTiming[index].endedAt === null) {
      stepTiming[index].endedAt = endedAt ? new Date(endedAt).getTime() : now;
    }
  }

  stepStates[index] = status;

  const item = stepTimeline.querySelector(`.step-item[data-index="${index}"]`);
  if (!item) return;
  item.className = `step-item ${status}`;

  const meta = item.querySelector(".step-meta");
  if (meta) {
    meta.textContent = buildMetaText(index, status, message, durationMs);
  }
}

function resetTimeline() {
  stepStates = new Array(STEP_TITLES.length).fill("pending");
  stepTiming = new Array(STEP_TITLES.length).fill(null).map(() => ({
    startedAt: null,
    endedAt: null,
  }));
  stepStreams = new Array(STEP_TITLES.length).fill("");
  renderTimeline();
}

function appendStepStream(index, text) {
  if (!stepTimeline) return;
  if (index < 0 || index >= STEP_TITLES.length) return;
  if (!text) return;

  const previous = stepStreams[index] || "";
  stepStreams[index] = previous ? `${previous}\n${text}` : text;

  const item = stepTimeline.querySelector(`.step-item[data-index="${index}"]`);
  if (!item) return;
  const streamNode = item.querySelector(".step-stream");
  if (!streamNode) return;
  streamNode.textContent = stepStreams[index];
}

function clearList(element) {
  while (element.firstChild) {
    element.removeChild(element.firstChild);
  }
}

function fillList(element, items) {
  clearList(element);
  if (!items || items.length === 0) {
    const li = document.createElement("li");
    li.textContent = "No items available.";
    element.appendChild(li);
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = String(item);
    element.appendChild(li);
  }
}

function fillWebReferences(findings) {
  clearList(webReferences);
  if (!findings || findings.length === 0) {
    const li = document.createElement("li");
    li.textContent = "No references returned.";
    webReferences.appendChild(li);
    return;
  }

  findings.forEach((f) => {
    const li = document.createElement("li");
    const title = f.title || "Untitled";
    const url = f.url || "";
    if (url) {
      const link = document.createElement("a");
      link.href = url;
      link.target = "_blank";
      link.rel = "noreferrer noopener";
      link.textContent = title;
      li.appendChild(link);
    } else {
      li.textContent = title;
    }
    webReferences.appendChild(li);
  });
}

function renderReport(report) {
  if (!report) return;
  reportSection.classList.remove("hidden");

  reportId.textContent = report.report_id || "-";
  outLogId.textContent = report.log_id || "-";
  service.textContent = report.impacted_service || "-";
  confidence.textContent =
    typeof report.root_cause_confidence === "number"
      ? report.root_cause_confidence.toFixed(2)
      : "-";
  issueSummary.textContent = report.issue_summary || "-";
  rootCause.textContent = report.probable_root_cause || "-";
  webSolution.textContent = report.web_probable_solution || "-";
  agentSolution.textContent = report.agent_intelligence_solution || "-";
  combinedSolution.textContent = report.probable_solution || "-";
  markdownOutput.textContent = report.markdown_report || "-";

  const suspected = Array.isArray(report.suspected_files)
    ? report.suspected_files[0] || {}
    : {};
  directory.textContent = suspected.directory || "-";
  filePath.textContent = suspected.file_path || "-";
  functionName.textContent = suspected.function_or_class || "-";
  lineNo.textContent =
    suspected.line_number !== null && suspected.line_number !== undefined
      ? String(suspected.line_number)
      : "-";

  fillList(nextActions, report.next_actions || []);
  fillList(preventiveActions, report.preventive_actions || []);

  const findings = report.evidence?.web_findings || [];
  fillWebReferences(findings);

  const isSuccess = report.status === "success";
  reportBadge.textContent = report.status || "unknown";
  reportBadge.className = `badge ${isSuccess ? "success" : "failed"}`;
}

function extractReportFromResponse(raw) {
  if (raw && typeof raw === "object" && raw.report) {
    return raw.report;
  }
  if (raw && typeof raw.response === "string") {
    try {
      const parsed = JSON.parse(raw.response);
      if (parsed && parsed.report) return parsed.report;
    } catch (_err) {
      return null;
    }
  }
  return null;
}

function parseSseBlock(block) {
  const lines = block.split(/\r?\n/);
  const dataLines = lines
    .map((line) => line.trimEnd())
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart());

  if (dataLines.length === 0) return null;
  const dataText = dataLines.join("\n");
  try {
    return JSON.parse(dataText);
  } catch (_err) {
    return null;
  }
}

function applyStreamEvent(event) {
  if (!event || typeof event !== "object") return;

  if (event.type === "report_generated" && event.report) {
    latestReport = event.report;
    return;
  }

  if (event.type === "report_ready") {
    latestReport = event.report || latestReport;
    latestStorage = event.storage || latestStorage;
    if (latestReport) {
      renderReport(latestReport);
      const stored = latestStorage?.s3?.stored;
      if (latestReport.status === "success") {
        setStatus(
          stored
            ? `RCA report generated and stored to S3: ${latestStorage.s3.s3_uri || ""}`
            : "RCA report generated successfully.",
          "success"
        );
      } else {
        setStatus("RCA completed with failure status. Check report details.", "error");
      }
    }
    return;
  }

  if (event.type === "stream_error") {
    setStatus(`Streaming error: ${event.error || "Unknown error"}`, "error");
    return;
  }

  const stepKey = event.step_key;
  if (!stepKey || !(stepKey in STEP_KEY_TO_INDEX)) return;
  const index = STEP_KEY_TO_INDEX[stepKey];

  if (event.type === "step_started") {
    setStepStatus(index, "loading", {
      startedAt: event.started_at || null,
    });
    return;
  }

  if (event.type === "step_completed") {
    setStepStatus(index, "completed", {
      startedAt: event.started_at || null,
      endedAt: event.ended_at || null,
      durationMs: event.duration_ms,
      message: event.message || "",
    });
    if (stepKey === "store_s3" && event.storage) {
      latestStorage = { s3: event.storage };
    }
    return;
  }

  if (event.type === "step_failed") {
    setStepStatus(index, "failed", {
      startedAt: event.started_at || null,
      endedAt: event.ended_at || null,
      durationMs: event.duration_ms,
      message: event.message || STEP_META.failed,
    });
    if (stepKey === "store_s3" && event.storage) {
      latestStorage = { s3: event.storage };
    }
    return;
  }

  if (event.type === "step_stream") {
    appendStepStream(index, String(event.text || ""));
  }
}

async function consumeStreamingResponse(response) {
  if (!response.body) {
    throw new Error("Streaming response body is unavailable.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  let gotReady = false;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    let separatorIndex = buffer.indexOf("\n\n");
    while (separatorIndex !== -1) {
      const block = buffer.slice(0, separatorIndex).trim();
      buffer = buffer.slice(separatorIndex + 2);
      const event = parseSseBlock(block);
      if (event) {
        applyStreamEvent(event);
        if (event.type === "report_ready") {
          gotReady = true;
        }
      }
      separatorIndex = buffer.indexOf("\n\n");
    }
  }

  if (!gotReady && latestReport) {
    renderReport(latestReport);
  }

  if (!gotReady && !latestReport) {
    throw new Error("Stream finished without a final report.");
  }
}

async function invokeFallbackJsonFromResponse(response) {
  const payload = await response.json();
  const report = extractReportFromResponse(payload);
  if (!report) throw new Error("Backend returned an unexpected response shape.");
  latestReport = report;
  renderReport(report);
  setStatus(
    report.status === "success"
      ? "RCA report generated successfully."
      : "RCA completed with failure status. Check report details.",
    report.status === "success" ? "success" : "error"
  );
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const logId = (logIdInput.value || "").trim().toUpperCase();

  ensureProgressVisible();
  resetTimeline();
  reportSection.classList.add("hidden");
  latestReport = null;
  latestStorage = null;

  if (!/^LOG-\d{4}$/.test(logId)) {
    setStepStatus(0, "failed", { message: "Invalid log format. Use LOG-####." });
    for (let i = 1; i < STEP_TITLES.length; i++) {
      setStepStatus(i, "skipped");
    }
    setStatus("Please enter a valid log ID in format LOG-####.", "error");
    return;
  }

  submitBtn.disabled = true;
  setStatus("Running live RCA stream...", "loading");

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ log_id: logId, stream: true }),
    });

    if (!response.ok) {
      throw new Error(`Backend returned HTTP ${response.status}`);
    }

    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("text/event-stream")) {
      await consumeStreamingResponse(response);
    } else {
      await invokeFallbackJsonFromResponse(response);
    }
  } catch (error) {
    const activeIndex = stepStates.findIndex((state) => state === "loading");
    if (activeIndex >= 0) {
      setStepStatus(activeIndex, "failed", { message: `Request error: ${error.message}` });
      for (let i = activeIndex + 1; i < STEP_TITLES.length; i++) {
        if (stepStates[i] === "pending") {
          setStepStatus(i, "skipped");
        }
      }
    }
    setStatus(
      `Request failed. Ensure backend is running on ${API_URL}. Error: ${error.message}`,
      "error"
    );
  } finally {
    submitBtn.disabled = false;
  }
});

resetTimeline();

if (!form || !stepTimeline || !progressSection) {
  setStatus(
    "UI did not load complete timeline components. Refresh and ensure latest frontend files are served.",
    "error"
  );
}
