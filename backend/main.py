# main.py
import io
import re
import uuid
from datetime import datetime

import cloudpickle
import handlers as broccoli_handlers
import numpy as np
import pandas as pd
import redis
from fastapi import Body, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from broccoli.core.result import ResultBackend
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue

# ----------------------------------------------------------------------
# Redis client for shared state
# ----------------------------------------------------------------------
# redis_client = RedisController(decode_responses=False).get_client()
redis_client = redis.Redis.from_url("redis://localhost:6379", decode_responses=False)
redis_client.flushdb()  # Clear Redis on startup for a clean slate (for testing/demo purposes)
result_backend = ResultBackend("redis://localhost:6379")

# In-memory dictionaries for dataset/preprocess IDs (lightweight metadata).
# These are just a *cache* now -- the source of truth is always Redis, so a
# server restart (or the in-memory dict simply not knowing about an id yet)
# never breaks a lookup. See _get_dataset_df / _get_preprocessed_bundle.
DATASETS: dict[str, dict] = {}  # metadata only
PREPROCESSED: dict[str, dict] = {}  # metadata only
JOBS: dict[str, dict] = {}  # job status (cached from Redis)

REDIS_TTL_SECONDS = 3600 * 24 * 7  # 7 days

app = FastAPI(title="Text Classification Model Trainer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cleanup functions (same as before)
URL_RE = re.compile(r"https?://\S+|www\.\S+")
HTML_RE = re.compile(r"<.*?>")
PUNCT_RE = re.compile(r"[^\w\s]")
NUMBER_RE = re.compile(r"\d+")
SPECIAL_RE = re.compile(r"[^a-zA-Z0-9\s]")


# ----------------------------------------------------------------------
# Shared helpers -- always read through to Redis so that neither a page
# reload nor a server restart loses track of data that is still cached.
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# Step 1: Upload
# ----------------------------------------------------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        decoded = contents.decode("utf-8")
        df = pd.read_csv(io.StringIO(decoded))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")

    if df.empty:
        raise HTTPException(status_code=400, detail="CSV is empty.")

    dataset_id = uuid.uuid4().hex
    # Store in Redis with TTL
    redis_client.setex(
        f"dataset:{dataset_id}", REDIS_TTL_SECONDS, cloudpickle.dumps(df)
    )
    # Keep metadata
    DATASETS[dataset_id] = {"row_count": len(df), "columns": df.columns.tolist()}

    return _dataset_summary(dataset_id, df)


# ----------------------------------------------------------------------
# Resume support: check whether a previously-uploaded dataset is still
# sitting in Redis (e.g. after the browser was closed/reloaded) so the
# frontend can skip re-uploading.
# ----------------------------------------------------------------------
@app.get("/dataset/{dataset_id}")
async def get_dataset(dataset_id: str):
    df = _get_dataset_df(dataset_id)
    return _dataset_summary(dataset_id, df)


# ----------------------------------------------------------------------
# Step 2: Preprocess (synchronous, using in-memory for simplicity)
# We could also offload to Broccoli, but for now keep it as is, storing result in Redis.
# ----------------------------------------------------------------------
@app.post("/preprocess")
async def preprocess_data(payload: dict = Body(...)):
    dataset_id = payload.get("dataset_id")
    config = payload.get("config", {})

    # Reads through to Redis, so this works even if DATASETS lost the entry.
    df = _get_dataset_df(dataset_id)

    text_column = config.get("text_column")
    label_column = config.get("label_column")
    if not text_column or not label_column:
        raise HTTPException(
            status_code=400, detail="text_column and label_column required."
        )

    # Drop columns
    drop_columns = [c for c in (config.get("drop_columns") or []) if c in df.columns]
    drop_columns = [c for c in drop_columns if c not in (text_column, label_column)]
    if drop_columns:
        df = df.drop(columns=drop_columns)

    before_sample = (
        str(df[text_column].dropna().iloc[0])
        if not df[text_column].dropna().empty
        else ""
    )
    df[text_column] = df[text_column].apply(lambda t: clean_text(t, config))
    after_sample = str(df[text_column].iloc[0]) if not df.empty else ""

    if config.get("remove_nulls", True):
        df = df[(df[text_column].str.strip() != "") & df[label_column].notna()]
    if config.get("remove_duplicates", True):
        df = df.drop_duplicates(subset=[text_column])
    if df.empty:
        raise HTTPException(
            status_code=400, detail="No rows remain after preprocessing."
        )
    if config.get("shuffle", True):
        seed = int(config.get("random_seed", 42))
        df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

    class_distribution = df[label_column].value_counts().to_dict()
    class_distribution = {str(k): int(v) for k, v in class_distribution.items()}

    # Store preprocessed data in Redis
    preprocessed_id = uuid.uuid4().hex
    redis_client.setex(
        f"preprocessed:{preprocessed_id}",
        REDIS_TTL_SECONDS,
        cloudpickle.dumps(
            {
                "df": df,
                "text_column": text_column,
                "label_column": label_column,
                "config": config,
                "before_sample": before_sample,
                "after_sample": after_sample,
            }
        ),
    )
    PREPROCESSED[preprocessed_id] = {
        "row_count": len(df),
        "class_distribution": class_distribution,
    }

    return {
        "preprocessed_id": preprocessed_id,
        "row_count": len(df),
        "before_text": before_sample,
        "after_text": after_sample,
        "class_distribution": class_distribution,
    }


# ----------------------------------------------------------------------
# Resume support: check whether previously-preprocessed data is still in
# Redis so the frontend can skip re-running preprocessing.
# ----------------------------------------------------------------------
@app.get("/preprocessed/{preprocessed_id}")
async def get_preprocessed(preprocessed_id: str):
    bundle = _get_preprocessed_bundle(preprocessed_id)
    df = bundle["df"]
    label_column = bundle["label_column"]
    class_distribution = df[label_column].value_counts().to_dict()
    class_distribution = {str(k): int(v) for k, v in class_distribution.items()}
    return {
        "preprocessed_id": preprocessed_id,
        "row_count": len(df),
        "class_distribution": class_distribution,
        "text_column": bundle["text_column"],
        "label_column": bundle["label_column"],
        "before_text": bundle.get("before_sample", ""),
        "after_text": bundle.get("after_sample", ""),
    }


# ----------------------------------------------------------------------
# Step 3: Train with fan-out and a single fan-in finalizer
# ----------------------------------------------------------------------
@app.post("/train")
async def train_model(payload: dict = Body(...)):
    preprocessed_id = payload.get("preprocessed_id")
    if not preprocessed_id:
        raise HTTPException(status_code=400, detail="preprocessed_id is required.")
    _get_preprocessed_bundle(preprocessed_id)

    primary_model = payload.get("model")
    if primary_model not in broccoli_handlers.MODEL_CATALOG:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unsupported primary model. Choose from: "
                f"{list(broccoli_handlers.MODEL_CATALOG.keys())}"
            ),
        )

    selected_params = payload.get("params") or {}
    test_size = float(payload.get("test_size", 0.25))
    random_state = int(payload.get("random_state", 42))
    max_features = int(payload.get("max_features", 5000))

    run_id = uuid.uuid4().hex
    load_task_id = uuid.uuid4().hex
    final_task_id = uuid.uuid4().hex

    catalog_entry = broccoli_handlers.MODEL_CATALOG[primary_model]
    alternatives = list(catalog_entry["alternatives"])
    grids = dict(catalog_entry.get("grids", {}))

    candidates = []
    for model_name in alternatives:
        parameter_sets = list(grids.get(model_name) or [])
        if model_name == primary_model:
            parameter_sets = [selected_params] + [
                params for params in parameter_sets if params != selected_params
            ]
        for params in parameter_sets:
            candidates.append(
                {
                    "task_id": uuid.uuid4().hex,
                    "model_name": model_name,
                    "params": params,
                }
            )

    if not candidates:
        raise HTTPException(
            status_code=400, detail="No model candidates were generated."
        )

    training_task_ids = [item["task_id"] for item in candidates]
    all_task_ids = [load_task_id, *training_task_ids, final_task_id]
    manifest = {
        "run_id": run_id,
        "status": "pending",
        "preprocessed_id": preprocessed_id,
        "load_task_id": load_task_id,
        "training_task_ids": training_task_ids,
        "training_tasks": candidates,
        "final_task_id": final_task_id,
        "task_ids": all_task_ids,
        "created_at": pd.Timestamp.now().isoformat(),
    }
    _save_run_manifest(run_id, manifest)

    queue = TaskQueue(redis_url="redis://localhost:6379", decode_responses=True)
    try:
        queue.push(
            Task(
                task_id=load_task_id,
                task_type="load_from_redis",
                payload={"preprocessed_id": preprocessed_id, "run_id": run_id},
            )
        )

        for candidate in candidates:
            queue.push(
                Task(
                    task_id=candidate["task_id"],
                    task_type="train_model",
                    payload={
                        "run_id": run_id,
                        "preprocessed_id": preprocessed_id,
                        "model_name": candidate["model_name"],
                        "params": candidate["params"],
                        "test_size": test_size,
                        "random_state": random_state,
                        "max_features": max_features,
                    },
                    depends_on=[load_task_id],
                )
            )

        queue.push(
            Task(
                task_id=final_task_id,
                task_type="evaluate_and_store",
                payload={
                    "run_id": run_id,
                    "final_task_id": final_task_id,
                    "training_task_ids": training_task_ids,
                },
                depends_on=training_task_ids,
            )
        )

        manifest["status"] = "running"
        _save_run_manifest(run_id, manifest)
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["submission_error"] = str(exc)
        _save_run_manifest(run_id, manifest)
        raise HTTPException(
            status_code=500, detail=f"Could not submit training tasks: {exc}"
        ) from exc

    return {
        "run_id": run_id,
        "job_id": run_id,
        "final_task_id": final_task_id,
        "task_ids": all_task_ids,
        "load_task_id": load_task_id,
        "training_tasks": candidates,
        "poll_interval_ms": 2000,
    }


@app.post("/tasks/status")
async def get_tasks_status(payload: dict = Body(...)):
    task_ids = payload.get("task_ids") or []
    if not isinstance(task_ids, list) or not task_ids:
        raise HTTPException(
            status_code=400, detail="task_ids must be a non-empty list."
        )

    snapshots = {task_id: _task_snapshot(task_id) for task_id in task_ids}
    return {
        "statuses": {
            task_id: snapshot["status"] for task_id, snapshot in snapshots.items()
        },
        "tasks": snapshots,
    }


@app.get("/job/{run_id}")
async def get_job_status(run_id: str):
    manifest = _load_run_manifest(run_id)
    if manifest is None:
        raise HTTPException(
            status_code=404, detail="Training run not found or expired."
        )

    result = _load_run_result(run_id)
    if result is not None:
        return {
            "run_id": run_id,
            "status": "completed",
            "final_task_id": manifest["final_task_id"],
            "results": result,
        }

    final_snapshot = _task_snapshot(manifest["final_task_id"])
    if final_snapshot["status"] in {"failed", "cancelled", "canceled"}:
        manifest["status"] = "failed"
        manifest["error"] = final_snapshot.get("error")
        _save_run_manifest(run_id, manifest)
        return {
            "run_id": run_id,
            "status": "failed",
            "final_task_id": manifest["final_task_id"],
            "error": final_snapshot.get("error"),
        }

    return {
        "run_id": run_id,
        "status": final_snapshot["status"],
        "final_task_id": manifest["final_task_id"],
    }


@app.get("/job/{run_id}/tasks")
async def get_job_task_statuses(run_id: str):
    manifest = _load_run_manifest(run_id)
    if manifest is None:
        raise HTTPException(
            status_code=404, detail="Training run not found or expired."
        )

    snapshots = [_task_snapshot(task_id) for task_id in manifest["task_ids"]]
    completed = sum(item["status"] == "completed" for item in snapshots)
    failures = [
        item
        for item in snapshots
        if item["status"] in {"failed", "cancelled", "canceled"}
    ]
    return {
        "run_id": run_id,
        "status": (
            "failed"
            if failures
            else "completed"
            if completed == len(snapshots)
            else "running"
        ),
        "completed": completed,
        "total": len(snapshots),
        "final_task_id": manifest["final_task_id"],
        "tasks": snapshots,
        "failures": failures,
    }


# ----------------------------------------------------------------------
# Download model (unchanged)
# ----------------------------------------------------------------------
@app.get("/download/{model_id}")
async def download_model(model_id: str, type: str = "model"):
    data = redis_client.get(f"model:{model_id}")
    if data is None:
        raise HTTPException(status_code=404, detail="Model not found.")
    info = cloudpickle.loads(data)
    if type == "vectorizer":
        obj = info["vectorizer"]
        filename = f"{model_id}_vectorizer.pkl"
    else:
        obj = info["model"]
        filename = f"{model_id}_model.pkl"
    buffer = io.BytesIO()
    cloudpickle.dump(obj, buffer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ----------------------------------------------------------------------
# Predict (unchanged)
# ----------------------------------------------------------------------
@app.post("/predict")
async def predict(payload: dict = Body(...)):
    model_id = payload.get("model_id")
    text = payload.get("text", "")
    if not text or not text.strip():
        raise HTTPException(status_code=400, detail="text is required.")
    data = redis_client.get(f"model:{model_id}")
    if data is None:
        raise HTTPException(status_code=404, detail="Model not found.")
    info = cloudpickle.loads(data)
    cfg = info.get("preprocess_config", {})
    cleaned = clean_text(text, cfg)
    vectorizer = info["vectorizer"]
    model = info["model"]
    X = vectorizer.transform([cleaned])
    pred = model.predict(X)[0]
    result = {"prediction": str(pred)}
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)[0]
        classes = [str(c) for c in model.classes_]
        result["probabilities"] = dict(zip(classes, [float(p) for p in proba]))
    return result


# ----------------------------------------------------------------------
# Serve frontend static files
# ----------------------------------------------------------------------
app.mount("/", StaticFiles(directory="../frontend", html=True), name="frontend")


def _dataset_summary(dataset_id: str, df: pd.DataFrame) -> dict:
    preview = df.head(10).where(pd.notnull(df.head(10)), None).to_dict(orient="records")
    columns = df.columns.tolist()
    types = df.dtypes.astype(str).to_dict()
    missing = df.isnull().sum().to_dict()
    numeric_df = df.select_dtypes(include=[np.number])
    stats = numeric_df.describe().to_dict() if not numeric_df.empty else {}
    return {
        "dataset_id": dataset_id,
        "preview": preview,
        "columns": columns,
        "types": types,
        "missing": missing,
        "stats": stats,
    }


def _get_dataset_df(dataset_id: str) -> pd.DataFrame:
    raw = redis_client.get(f"dataset:{dataset_id}")
    if raw is None:
        raise HTTPException(status_code=404, detail="Dataset not found or expired.")
    df = cloudpickle.loads(raw)
    DATASETS[dataset_id] = {"row_count": len(df), "columns": df.columns.tolist()}
    return df


def _get_preprocessed_bundle(preprocessed_id: str) -> dict:
    raw = redis_client.get(f"preprocessed:{preprocessed_id}")
    if raw is None:
        raise HTTPException(
            status_code=404, detail="Preprocessed dataset not found or expired."
        )
    bundle = cloudpickle.loads(raw)
    df = bundle["df"]
    label_column = bundle["label_column"]
    class_distribution = df[label_column].value_counts().to_dict()
    class_distribution = {str(k): int(v) for k, v in class_distribution.items()}
    PREPROCESSED[preprocessed_id] = {
        "row_count": len(df),
        "class_distribution": class_distribution,
    }
    return bundle


def _load_run_manifest(run_id: str) -> dict | None:
    raw = redis_client.get(f"run:{run_id}")
    return cloudpickle.loads(raw) if raw is not None else None


def _save_run_manifest(run_id: str, manifest: dict) -> None:
    redis_client.setex(f"run:{run_id}", REDIS_TTL_SECONDS, cloudpickle.dumps(manifest))


def _load_run_result(run_id: str) -> dict | None:
    raw = redis_client.get(f"result:{run_id}")
    return cloudpickle.loads(raw) if raw is not None else None


def _task_snapshot(task_id: str) -> dict:
    queue = TaskQueue(redis_url="redis://localhost:6379", decode_responses=True)
    task = queue.get_task(task_id)
    if task is None:
        task = result_backend.get_task_result(task_id)
        if task is None:
            return {"task_id": task_id, "status": "pending", "error": None}
    print(isinstance(task, dict))
    if isinstance(task, dict):
        status = task.get("status", "unknown")
        error = task.get("error")
    else:
        status = task.status
        error = task.error
    print(f"Task snapshot for {task_id}: status={status}, error={error}")

    return {"task_id": task_id, "status": str(status).lower(), "error": error}


def clean_text(text, cfg: dict) -> str:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return ""
    text = str(text)

    if cfg.get("remove_html", True):
        text = HTML_RE.sub(" ", text)
    if cfg.get("remove_urls", True):
        text = URL_RE.sub(" ", text)

    text = text.lower()

    if cfg.get("remove_punctuation", True):
        text = PUNCT_RE.sub(" ", text)
    if cfg.get("remove_numbers", True):
        text = NUMBER_RE.sub(" ", text)
    if cfg.get("remove_special_chars", True):
        text = SPECIAL_RE.sub(" ", text)

    text = re.sub(r"\s+", " ", text).strip()
    return text
