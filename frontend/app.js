const API_URL = "http://127.0.0.1:8080/invocations";

const form = document.getElementById("rca-form");
const logIdInput = document.getElementById("logId");
const submitBtn = document.getElementById("submitBtn");
const statusBox = document.getElementById("status");
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

function setStatus(message, kind) {
  statusBox.className = `status ${kind}`;
  statusBox.textContent = message;
  statusBox.classList.remove("hidden");
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
      if (parsed && parsed.report) {
        return parsed.report;
      }
    } catch (_err) {
      return null;
    }
  }

  return null;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const logId = (logIdInput.value || "").trim().toUpperCase();

  if (!/^LOG-\d{4}$/.test(logId)) {
    setStatus("Please enter a valid log ID in format LOG-####.", "error");
    return;
  }

  submitBtn.disabled = true;
  setStatus("Running RCA analysis. This may take a few seconds...", "loading");

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ log_id: logId }),
    });

    const payload = await response.json();
    const report = extractReportFromResponse(payload);
    if (!report) {
      throw new Error("Backend returned an unexpected response shape.");
    }

    renderReport(report);
    if (report.status === "success") {
      setStatus("RCA report generated successfully.", "success");
    } else {
      setStatus("RCA completed with failure status. Check report details.", "error");
    }
  } catch (error) {
    setStatus(
      `Request failed. Ensure backend is running on ${API_URL}. Error: ${error.message}`,
      "error"
    );
  } finally {
    submitBtn.disabled = false;
  }
});

