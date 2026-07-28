(function () {
  const API_BASE = "http://localhost:8000"; // Change as needed
  const STORAGE_KEY = "mlTrainerState_v1";

  // ---- Global state (just IDs, not data) ----
  let datasetId = null;
  let preprocessedId = null;
  let trainingRunId = null;
  let trainingTaskIds = [];
  let finalTaskId = null;
  let trainingResults = null;
  let pollingTimeout = null;
  let pollInFlight = false;

  // ---- Session persistence (survives browser restarts) ----
  // We only ever store IDs here, never data. On load we ask the backend
  // whether Redis still has the underlying data for each ID before
  // trusting it -- see restoreSession().
  function saveState() {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          datasetId,
          datasetColumns: window.datasetColumns,
          preprocessedId,
          trainingRunId,
          trainingTaskIds,
          finalTaskId,
          trainingResults,
        }),
      );
    } catch (e) {
      console.warn("Could not save session state:", e);
    }
  }

  function loadSavedState() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function clearSavedState() {
    localStorage.removeItem(STORAGE_KEY);
  }

  // ---- Navigation (unchanged) ----
  const navItems = document.querySelectorAll(".nav-item");
  const stepContainers = document.querySelectorAll(".step-container");
  navItems.forEach((item) => {
    item.addEventListener("click", () => {
      const step = item.dataset.step;
      navItems.forEach((n) => n.classList.remove("active"));
      item.classList.add("active");
      stepContainers.forEach((c) => c.classList.remove("active"));
      document.getElementById(`step-${step}`).classList.add("active");
    });
  });

  const resetBtn = document.getElementById("reset-session-btn");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      stopTrainingPolling();
      clearSavedState();
      location.reload();
    });
  }

  // ---- Utility: disable/enable button with loading text ----
  function setButtonLoading(btn, isLoading, text = "Processing...") {
    btn.disabled = isLoading;
    btn.innerText = isLoading ? text : btn.dataset.originalText;
    if (!btn.dataset.originalText) btn.dataset.originalText = btn.innerText;
  }

  // ---- Step 1: Upload Data ----
  const fileInput = document.getElementById("csv-file");
  const uploadStatus = document.getElementById("upload-status");
  const dataPreview = document.getElementById("data-preview");

  fileInput.addEventListener("change", async function (e) {
    const file = e.target.files[0];
    if (!file) return;

    e.preventDefault();

    uploadStatus.innerHTML =
      '<div class="alert alert-info">Uploading file...</div>';
    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch(`${API_BASE}/upload`, {
        method: "POST",
        body: formData,
      });
      if (!response.ok) throw new Error(await response.text());
      const result = await response.json();

      // Fresh upload invalidates anything downstream that was tied to the
      // previous dataset.
      preprocessedId = null;
      trainingRunId = null;
      trainingTaskIds = [];
      finalTaskId = null;
      trainingResults = null;
      stopTrainingPolling();

      datasetId = result.dataset_id;
      uploadStatus.innerHTML =
        '<div class="alert alert-success">✅ File uploaded successfully!</div>';

      if (result.columns) {
        window.datasetColumns = result.columns;
        populateDataPreview(result);
        dataPreview.style.display = "block";
        updatePreprocVisibility(); // show preproc content now that datasetId is set
      } else {
        // fallback: just enable next steps, columns will be fetched separately
        dataPreview.style.display = "none";
        updatePreprocVisibility();
      }
      updateTrainVisibility();
      updateEvalVisibility();
      saveState();
    } catch (err) {
      uploadStatus.innerHTML = `<div class="alert alert-error">Upload failed: ${err.message}</div>`;
      dataPreview.style.display = "none";
    }
  });

  function populateDataPreview(data) {
    // data should contain: columns, preview (array of objects), types, missing counts, stats
    const { columns, preview, types, missing, stats } = data;
    // Render table
    const previewTable = document.getElementById("preview-table");
    let html = "<table><thead><tr>";
    columns.forEach((col) => (html += `<th>${col}</th>`));
    html += "</tr></thead><tbody>";
    preview.forEach((row) => {
      html += "<tr>";
      columns.forEach((col) => (html += `<td>${row[col] ?? ""}</td>`));
      html += "</tr>";
    });
    html += "</tbody></table>";
    previewTable.innerHTML = html;

    // Column list
    document.getElementById("column-list").innerHTML = columns
      .map((c) => `<li>${c}</li>`)
      .join("");

    // Data types & missing
    const dtypeInfo = document.getElementById("dtype-info");
    let dtypeHtml =
      "<table><tr><th>Column</th><th>Type</th><th>Missing</th></tr>";
    columns.forEach((col) => {
      dtypeHtml += `<tr><td>${col}</td><td>${types[col]}</td><td>${missing[col]}</td></tr>`;
    });
    dtypeHtml += "</table>";
    dtypeInfo.innerHTML = dtypeHtml;

    // Stats
    const statsDiv = document.getElementById("stats-table");
    let statsHtml =
      "<table><tr><th>Column</th><th>Mean</th><th>Min</th><th>Max</th></tr>";
    if (stats) {
      columns.forEach((col) => {
        if (stats[col]) {
          statsHtml += `<tr><td>${col}</td><td>${stats[col].mean?.toFixed(2) ?? "-"}</td><td>${stats[col].min?.toFixed(2) ?? "-"}</td><td>${stats[col].max?.toFixed(2) ?? "-"}</td></tr>`;
        }
      });
    }
    statsHtml += "</table>";
    statsDiv.innerHTML = statsHtml;
  }

  // ---- Step 2: Preprocessing ----
  // Enable content only if dataset is uploaded
  function updatePreprocVisibility() {
    const warning = document.getElementById("preproc-warning");
    const content = document.getElementById("preproc-content");
    if (datasetId) {
      warning.style.display = "none";
      content.style.display = "block";
      // Fetch column names from backend if not already available (we can store them globally)
      // Assuming we have stored columns from upload. For simplicity, call /dataset/{id}/columns
      fetchDatasetColumns();
    } else {
      warning.style.display = "block";
      content.style.display = "none";
    }
  }

  async function fetchDatasetColumns() {
    // You can implement this if upload doesn't return columns.
    // For now we'll assume columns are stored in a global variable after upload.
    // We'll simulate by reusing the columns array we saved earlier.
    // I'll add a global variable "columns" that gets populated.
    if (!window.datasetColumns) return;
    const cols = window.datasetColumns;
    const textSelect = document.getElementById("text-column-select");
    const labelSelect = document.getElementById("label-column-select");
    textSelect.innerHTML = cols.map((c) => `<option>${c}</option>`).join("");
    labelSelect.innerHTML = cols.map((c) => `<option>${c}</option>`).join("");
    const dropContainer = document.getElementById("drop-columns-container");
    dropContainer.innerHTML = cols
      .map((col) => {
        return `<label style="display:inline-block; margin-right:1rem;"><input type="checkbox" class="drop-col-checkbox" value="${col}" /> ${col}</label>`;
      })
      .join("");
  }

  document
    .getElementById("preview-preprocessing")
    .addEventListener("click", async function () {
      if (!datasetId) return;
      const btn = document.getElementById("preview-preprocessing");
      setButtonLoading(btn, true, "Processing...");

      const config = {
        text_column: document.getElementById("text-column-select").value,
        label_column: document.getElementById("label-column-select").value,
        drop_columns: Array.from(
          document.querySelectorAll(".drop-col-checkbox:checked"),
        ).map((cb) => cb.value),
        remove_urls: document.getElementById("remove-urls").checked,
        remove_html: document.getElementById("remove-html").checked,
        remove_punctuation:
          document.getElementById("remove-punctuation").checked,
        remove_numbers: document.getElementById("remove-numbers").checked,
        remove_special_chars: document.getElementById("remove-special-chars")
          .checked,
        remove_duplicates: document.getElementById("remove-duplicates").checked,
        remove_nulls: document.getElementById("remove-nulls").checked,
        shuffle: document.getElementById("shuffle-data").checked,
        random_seed:
          parseInt(document.getElementById("random-seed").value) || 42,
      };

      try {
        const response = await fetch(`${API_BASE}/preprocess`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dataset_id: datasetId, config }),
        });
        if (!response.ok) throw new Error(await response.text());
        const result = await response.json();
        preprocessedId = result.preprocessed_id;

        // Fresh preprocessing invalidates any training tied to the old data.
        trainingRunId = null;
        trainingTaskIds = [];
        finalTaskId = null;
        trainingResults = null;
        stopTrainingPolling();

        document.getElementById("preproc-results").style.display = "block";
        document.getElementById("preproc-status").innerText =
          `✅ Preprocessing complete!`;

        // Show before/after texts (backend should return samples)
        if (result.before_text)
          document.getElementById("before-text").innerText =
            result.before_text.substring(0, 500);
        if (result.after_text)
          document.getElementById("after-text").innerText =
            result.after_text.substring(0, 500);

        // Class distribution chart
        if (result.class_distribution) {
          drawClassDistribution(result.class_distribution);
        }
        // Enable training step
        updateTrainVisibility();
        updateEvalVisibility();
        saveState();
      } catch (err) {
        document.getElementById("preproc-results").style.display = "none";
        alert("Preprocessing error: " + err.message);
      } finally {
        setButtonLoading(btn, false);
      }
    });

  function drawClassDistribution(dist) {
    const labels = Object.keys(dist);
    const values = Object.values(dist);
    const ctx = document.getElementById("class-dist-chart").getContext("2d");
    if (window.classDistChart) window.classDistChart.destroy();
    window.classDistChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          { label: "Count", data: values, backgroundColor: "#4f46e5" },
        ],
      },
      options: { responsive: true, maintainAspectRatio: false },
    });
  }

  function updateTrainVisibility() {
    const warn = document.getElementById("train-warning");
    const content = document.getElementById("train-content");
    if (preprocessedId) {
      warn.style.display = "none";
      content.style.display = "block";
      document.getElementById("dataset-ready-msg").innerText =
        `Dataset ready for training.`;
    } else {
      warn.style.display = "block";
      content.style.display = "none";
    }
  }

  // ---- Step 3: Train Model ----
  const modelChoice = document.getElementById("model-choice");
  const paramsContainer = document.getElementById("model-params-container");
  function updateModelParams() {
    // (unchanged, same as original JS for dynamic params, but we'll collect values at submit)
    const choice = modelChoice.value;
    let html = "";
    if (choice === "Logistic Regression") {
      html = `<div class="row"><div class="col"><label>Max iterations <input type="number" id="lr-max-iter" value="1000" min="100" max="10000" step="100" /></label></div><div class="col"><label>Regularization C <input type="number" id="lr-c" value="1.0" min="0.01" max="10.0" step="0.1" /></label></div></div>`;
    } else if (choice === "Decision Tree") {
      html = `<div class="row"><div class="col"><label>Max depth <input type="number" id="dt-max-depth" value="10" min="1" max="50" /></label></div><div class="col"><label>Min samples split <input type="number" id="dt-min-samples" value="2" min="2" max="20" /></label></div></div>`;
    } else if (choice === "Gradient Boosting") {
      html = `<div class="row"><div class="col"><label>N estimators <input type="number" id="gb-n-est" value="100" min="10" max="500" step="10" /></label></div><div class="col"><label>Learning rate <input type="number" id="gb-lr" value="0.1" min="0.01" max="1.0" step="0.01" /></label></div><div class="col"><label>Max depth <input type="number" id="gb-max-depth" value="3" min="1" max="20" /></label></div></div>`;
    } else if (choice === "Random Forest") {
      html = `<div class="row"><div class="col"><label>N estimators <input type="number" id="rf-n-est" value="100" min="10" max="500" step="10" /></label></div><div class="col"><label>Max depth (0=None) <input type="number" id="rf-max-depth" value="0" min="0" max="50" /></label></div></div>`;
    }
    paramsContainer.innerHTML = html;
  }
  modelChoice.addEventListener("change", updateModelParams);
  updateModelParams();

  const testSizeSlider = document.getElementById("test-size");
  const testSizeVal = document.getElementById("test-size-val");
  testSizeSlider.addEventListener(
    "input",
    () => (testSizeVal.innerText = testSizeSlider.value + "%"),
  );

  document
    .getElementById("train-model-btn")
    .addEventListener("click", async function () {
      if (!preprocessedId) {
        alert("Please complete preprocessing first.");
        return;
      }
      const btn = document.getElementById("train-model-btn");
      setButtonLoading(btn, true, "Starting training...");

      const model = modelChoice.value;
      const testSize = parseInt(testSizeSlider.value) / 100;
      const randomState =
        parseInt(document.getElementById("train-random-state").value) || 42;
      const maxFeatures =
        parseInt(document.getElementById("max-features").value) || 5000;

      // Collect model-specific params
      const modelParams = {};
      if (model === "Logistic Regression") {
        modelParams.max_iter = parseInt(
          document.getElementById("lr-max-iter").value,
        );
        modelParams.C = parseFloat(document.getElementById("lr-c").value);
      } else if (model === "Decision Tree") {
        modelParams.max_depth = parseInt(
          document.getElementById("dt-max-depth").value,
        );
        modelParams.min_samples_split = parseInt(
          document.getElementById("dt-min-samples").value,
        );
      } else if (model === "Gradient Boosting") {
        modelParams.n_estimators = parseInt(
          document.getElementById("gb-n-est").value,
        );
        modelParams.learning_rate = parseFloat(
          document.getElementById("gb-lr").value,
        );
        modelParams.max_depth = parseInt(
          document.getElementById("gb-max-depth").value,
        );
      } else if (model === "Random Forest") {
        modelParams.n_estimators = parseInt(
          document.getElementById("rf-n-est").value,
        );
        modelParams.max_depth =
          parseInt(document.getElementById("rf-max-depth").value) || null;
      }

      const requestBody = {
        preprocessed_id: preprocessedId,
        model: model,
        params: modelParams,
        test_size: testSize,
        random_state: randomState,
        max_features: maxFeatures,
      };

      try {
        const response = await fetch(`${API_BASE}/train`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
        });
        if (!response.ok) throw new Error(await response.text());
        const result = await response.json();

        trainingRunId = result.run_id;
        trainingTaskIds = result.task_ids || [];
        finalTaskId = result.final_task_id;
        trainingResults = null;
        saveState();
        document.getElementById("training-results").style.display = "none";
        pollTrainingRun({
          runId: trainingRunId,
          taskIds: trainingTaskIds,
          finalTaskId,
          intervalMs: result.poll_interval_ms || 2000,
        });
      } catch (err) {
        alert("Training request failed: " + err.message);
      } finally {
        setButtonLoading(btn, false);
      }
    });

  function setTrainingProgress(html) {
    const el = document.getElementById("training-progress");
    if (el) el.innerHTML = html;
  }

  function stopTrainingPolling() {
    if (pollingTimeout) {
      clearTimeout(pollingTimeout);
      pollingTimeout = null;
    }
    pollInFlight = false;
  }

  async function pollTrainingRun({
    runId,
    taskIds,
    finalTaskId,
    intervalMs = 2000,
  }) {
    stopTrainingPolling();

    const poll = async () => {
      if (pollInFlight) return;
      pollInFlight = true;
      try {
        const response = await fetch(`${API_BASE}/tasks/status`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ task_ids: taskIds }),
        });
        if (!response.ok) throw new Error(await response.text());

        const data = await response.json();
        const statuses = data.statuses || {};
        const values = taskIds.map((taskId) => statuses[taskId] || "pending");
        const completedCount = values.filter(
          (status) => status === "completed",
        ).length;
        const failedTaskIds = taskIds.filter((taskId) =>
          ["failed", "cancelled", "canceled"].includes(statuses[taskId]),
        );

        if (failedTaskIds.length > 0) {
          stopTrainingPolling();
          setTrainingProgress(
            `<div class="alert alert-error">Training failed. Failed task(s): ${failedTaskIds.join(", ")}</div>`,
          );
          return;
        }

        setTrainingProgress(
          `<div class="alert alert-info">Training in progress: ${completedCount}/${taskIds.length} tasks complete.</div>`,
        );

        if (statuses[finalTaskId] === "completed") {
          const finalResponse = await fetch(`${API_BASE}/job/${runId}`);
          if (!finalResponse.ok) throw new Error(await finalResponse.text());
          const finalData = await finalResponse.json();
          if (finalData.status === "completed") {
            stopTrainingPolling();
            trainingResults = finalData.results;
            setTrainingProgress("");
            displayTrainingResults(trainingResults);
            updateEvalVisibility();
            saveState();
            return;
          }
        }

        pollInFlight = false;
        pollingTimeout = setTimeout(poll, intervalMs);
      } catch (error) {
        pollInFlight = false;
        console.error("Polling error:", error);
        setTrainingProgress(
          `<div class="alert alert-warning">Could not check training status. Retrying automatically.</div>`,
        );
        pollingTimeout = setTimeout(poll, intervalMs);
      }
    };

    poll();
  }

  function displayTrainingResults(results) {
    document.getElementById("training-results").style.display = "block";
    document.getElementById("train-acc").innerText =
      results.train_acc.toFixed(4);
    document.getElementById("test-acc").innerText = results.test_acc.toFixed(4);
    document.getElementById("class-report").innerText = results.report;
    // Draw confusion matrix from returned matrix data
    if (results.confusion_matrix && results.labels) {
      drawConfusionMatrixFromData(
        "confusion-matrix-chart",
        results.confusion_matrix,
        results.labels,
      );
    }
  }

  function drawConfusionMatrixFromData(canvasId, matrixData, labels) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    canvas.width = canvas.clientWidth;
    canvas.height = 300;
    const size = labels.length;
    const cellW = canvas.width / (size + 1);
    const cellH = canvas.height / (size + 1);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.font = "12px sans-serif";
    // matrixData expected as 2D array or object
    for (let i = 0; i < size; i++) {
      for (let j = 0; j < size; j++) {
        const val = matrixData[i][j];
        const intensity =
          Math.min(val / (Math.max(...matrixData.flat()) || 1), 1) * 200;
        ctx.fillStyle = `rgba(79, 70, 229, ${intensity / 255})`;
        ctx.fillRect((j + 1) * cellW, (i + 1) * cellH, cellW, cellH);
        ctx.fillStyle = "#000";
        ctx.fillText(
          val,
          (j + 1) * cellW + cellW / 2 - 5,
          (i + 1) * cellH + cellH / 2 + 4,
        );
      }
    }
    ctx.fillStyle = "#000";
    for (let i = 0; i < size; i++) {
      ctx.fillText(labels[i], (i + 1) * cellW + cellW / 4, cellH / 2);
      ctx.save();
      ctx.translate(cellW / 2, (i + 1) * cellH + cellH / 2);
      ctx.rotate(-Math.PI / 2);
      ctx.fillText(labels[i], 0, 0);
      ctx.restore();
    }
  }

  function updateEvalVisibility() {
    const warn = document.getElementById("eval-warning");
    const content = document.getElementById("eval-content");
    if (trainingResults) {
      warn.style.display = "none";
      content.style.display = "block";
      document.getElementById("eval-model-name").innerText =
        `Model ready: ${trainingResults.model_name}`;
      document.getElementById("eval-train-acc").innerText =
        trainingResults.train_acc.toFixed(4);
      document.getElementById("eval-test-acc").innerText =
        trainingResults.test_acc.toFixed(4);
      document.getElementById("eval-class-report").innerText =
        trainingResults.report;
      drawConfusionMatrixFromData(
        "eval-confusion-chart",
        trainingResults.confusion_matrix,
        trainingResults.labels,
      );
      // Enable download buttons (now real)
      const downloadModelBtn = document.getElementById("download-model");
      const downloadVecBtn = document.getElementById("download-vectorizer");
      downloadModelBtn.disabled = false;
      downloadVecBtn.disabled = false;
      downloadModelBtn.onclick = () =>
        window.open(
          `${API_BASE}/download/${trainingResults.model_id}`,
          "_blank",
        );
      downloadVecBtn.onclick = () =>
        window.open(
          `${API_BASE}/download/${trainingResults.model_id}?type=vectorizer`,
          "_blank",
        ); // or separate endpoint
    } else {
      warn.style.display = "block";
      content.style.display = "none";
    }
  }

  // ---- Step 5: Test Model ----
  document
    .getElementById("predict-btn")
    .addEventListener("click", async function () {
      const text = document.getElementById("test-input-text").value.trim();
      if (!text) {
        document.getElementById("prediction-result").innerHTML =
          '<div class="alert alert-warning">Please enter some text.</div>';
        return;
      }
      if (!trainingResults || !trainingResults.model_id) {
        document.getElementById("prediction-result").innerHTML =
          '<div class="alert alert-warning">No trained model available. Train a model first or upload one manually.</div>';
        return;
      }
      const resultDiv = document.getElementById("prediction-result");
      resultDiv.innerHTML = '<div class="alert alert-info">Predicting...</div>';
      try {
        const response = await fetch(`${API_BASE}/predict`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ model_id: trainingResults.model_id, text }),
        });
        if (!response.ok) throw new Error(await response.text());
        const data = await response.json();
        resultDiv.innerHTML = `<div class="alert alert-success"><strong>Prediction:</strong> ${data.prediction}</div>`;
      } catch (err) {
        resultDiv.innerHTML = `<div class="alert alert-error">Prediction failed: ${err.message}</div>`;
      }
    });

  // Expose columns to global after upload (store from /upload response)
  window.datasetColumns = null;

  // ---- Resume a previous session ----
  // On load, check localStorage for IDs from a previous visit, then ask
  // the backend whether Redis still actually has that data before
  // restoring any UI state. This means closing/reloading the browser
  // mid-workflow (or even mid-training-job) doesn't force a restart.
  async function restoreSession() {
    const saved = loadSavedState();
    if (!saved) {
      updatePreprocVisibility();
      updateTrainVisibility();
      updateEvalVisibility();
      return;
    }

    // Dataset
    if (saved.datasetId) {
      try {
        const resp = await fetch(`${API_BASE}/dataset/${saved.datasetId}`);
        if (resp.ok) {
          const data = await resp.json();
          datasetId = saved.datasetId;
          window.datasetColumns = data.columns;
          populateDataPreview(data);
          dataPreview.style.display = "block";
          uploadStatus.innerHTML =
            '<div class="alert alert-success">✅ Restored your previous dataset.</div>';
        }
      } catch (e) {
        console.warn("Could not restore dataset:", e);
      }
    }
    updatePreprocVisibility();

    // Preprocessed data (only meaningful if the dataset itself came back)
    if (datasetId && saved.preprocessedId) {
      try {
        const resp = await fetch(
          `${API_BASE}/preprocessed/${saved.preprocessedId}`,
        );
        if (resp.ok) {
          const data = await resp.json();
          preprocessedId = saved.preprocessedId;
          document.getElementById("preproc-results").style.display = "block";
          document.getElementById("preproc-status").innerText =
            `✅ Restored previous preprocessing (${data.row_count} rows).`;
          if (data.before_text)
            document.getElementById("before-text").innerText =
              data.before_text.substring(0, 500);
          if (data.after_text)
            document.getElementById("after-text").innerText =
              data.after_text.substring(0, 500);
          if (data.class_distribution)
            drawClassDistribution(data.class_distribution);
        }
      } catch (e) {
        console.warn("Could not restore preprocessed data:", e);
      }
    }
    updateTrainVisibility();

    // Training run / results (only meaningful if preprocessing came back)
    if (preprocessedId && saved.trainingRunId && saved.finalTaskId) {
      try {
        const resp = await fetch(`${API_BASE}/job/${saved.trainingRunId}`);
        if (resp.ok) {
          const data = await resp.json();
          trainingRunId = saved.trainingRunId;
          trainingTaskIds = saved.trainingTaskIds || [];
          finalTaskId = saved.finalTaskId;

          if (data.status === "completed") {
            trainingResults = data.results;
            document.getElementById("training-results").style.display = "none";
            displayTrainingResults(trainingResults);
          } else if (
            !["failed", "cancelled", "canceled"].includes(data.status) &&
            trainingTaskIds.length > 0
          ) {
            document.getElementById("training-results").style.display = "none";
            pollTrainingRun({
              runId: trainingRunId,
              taskIds: trainingTaskIds,
              finalTaskId,
              intervalMs: 2000,
            });
          } else {
            setTrainingProgress(
              `<div class="alert alert-error">The restored training run failed.</div>`,
            );
          }
        }
      } catch (e) {
        console.warn("Could not restore training run:", e);
      }
    }
    updateEvalVisibility();

    saveState();
  }

  restoreSession();
})();
