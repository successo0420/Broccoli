# broccoli_handlers.py
import logging
import uuid

import cloudpickle
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier

from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.core.task.task_registry import TaskRegistry

logger = logging.getLogger(__name__)
registry = TaskRegistry()
redis_client = RedisController(decode_responses=False).get_client()
result_backend = ResultBackend(
    "redis://localhost:6379"
)  # needed for evaluate_and_store

# ----------------------------------------------------------------------
# Model catalog: primary model -> list of alternatives + default grids
# ----------------------------------------------------------------------
MODEL_CATALOG = {
    "Logistic Regression": {
        "alternatives": ["Logistic Regression", "Decision Tree", "Gradient Boosting"],
        "grids": {
            "Logistic Regression": [
                {"max_iter": 1000, "C": 1.0},
                {"max_iter": 500, "C": 0.5},
            ],
            "Decision Tree": [
                {"max_depth": 10, "min_samples_split": 2},
                {"max_depth": 5, "min_samples_split": 5},
            ],
            "Gradient Boosting": [
                {"n_estimators": 100, "learning_rate": 0.1, "max_depth": 3},
                {"n_estimators": 200, "learning_rate": 0.05, "max_depth": 4},
            ],
        },
    },
    "Decision Tree": {
        "alternatives": ["Decision Tree", "Random Forest", "Gradient Boosting"],
        "grids": {
            "Decision Tree": [
                {"max_depth": 10, "min_samples_split": 2},
                {"max_depth": 5, "min_samples_split": 5},
            ],
            "Random Forest": [
                {"n_estimators": 100, "max_depth": 10},
                {"n_estimators": 200, "max_depth": None},
            ],
            "Gradient Boosting": [
                {"n_estimators": 100, "learning_rate": 0.1, "max_depth": 3},
            ],
        },
    },
    "Gradient Boosting": {
        "alternatives": ["Gradient Boosting", "XGBoost", "Random Forest"],
        "grids": {
            "Gradient Boosting": [
                {"n_estimators": 100, "learning_rate": 0.1, "max_depth": 3},
                {"n_estimators": 200, "learning_rate": 0.05, "max_depth": 4},
            ],
            "Random Forest": [
                {"n_estimators": 100, "max_depth": 10},
                {"n_estimators": 200, "max_depth": None},
            ],
            # XGBoost would need the xgboost library installed; we'll keep it as placeholder
        },
    },
    "Random Forest": {
        "alternatives": ["Random Forest", "Gradient Boosting", "Decision Tree"],
        "grids": {
            "Random Forest": [
                {"n_estimators": 100, "max_depth": 10},
                {"n_estimators": 200, "max_depth": None},
            ],
            "Gradient Boosting": [
                {"n_estimators": 100, "learning_rate": 0.1, "max_depth": 3},
            ],
            "Decision Tree": [
                {"max_depth": 10, "min_samples_split": 2},
            ],
        },
    },
}


def build_model(name: str, params: dict):
    """Instantiate a sklearn model from its name and parameters."""
    params = params or {}
    if name == "Logistic Regression":
        return LogisticRegression(
            max_iter=int(params.get("max_iter", 1000)),
            C=float(params.get("C", 1.0)),
        )
    if name == "Decision Tree":
        return DecisionTreeClassifier(
            max_depth=int(params.get("max_depth", 10)) or None,
            min_samples_split=int(params.get("min_samples_split", 2)),
        )
    if name == "Gradient Boosting":
        return GradientBoostingClassifier(
            n_estimators=int(params.get("n_estimators", 100)),
            learning_rate=float(params.get("learning_rate", 0.1)),
            max_depth=int(params.get("max_depth", 3)),
        )
    if name == "Random Forest":
        max_depth = int(params.get("max_depth", 0) or 0)
        return RandomForestClassifier(
            n_estimators=int(params.get("n_estimators", 100)),
            max_depth=max_depth or None,
        )
    raise ValueError(f"Unknown model type: {name}")


# ----------------------------------------------------------------------
# Broccoli task handlers
# ----------------------------------------------------------------------


@registry.register("load_from_redis")
def load_from_redis(payload: dict) -> dict:
    """Load preprocessed data from Redis."""
    preprocessed_id = payload["preprocessed_id"]
    key = f"preprocessed:{preprocessed_id}"
    data = redis_client.get(key)
    if data is None:
        raise ValueError(f"Preprocessed data {preprocessed_id} not found in Redis")
    return cloudpickle.loads(data)


@registry.register("preprocess_data")
def preprocess_data(payload: dict) -> dict:
    """
    Preprocess the dataset according to the config.
    This is essentially the same logic as the /preprocess endpoint,
    but we run it as a Broccoli task so it can be retried and tracked.
    """
    dataset_id = payload["dataset_id"]
    config = payload.get("config", {})
    # Load raw dataset from Redis
    raw_key = f"dataset:{dataset_id}"
    raw_data = redis_client.get(raw_key)
    if raw_data is None:
        raise ValueError(f"Dataset {dataset_id} not found")
    df = cloudpickle.loads(raw_data)

    text_column = config.get("text_column")
    label_column = config.get("label_column")
    if not text_column or not label_column:
        raise ValueError("text_column and label_column required")

    # Drop columns (except text and label)
    drop_columns = [c for c in (config.get("drop_columns") or []) if c in df.columns]
    drop_columns = [c for c in drop_columns if c not in (text_column, label_column)]
    if drop_columns:
        df = df.drop(columns=drop_columns)

    # Clean text using the same logic as before (you can import clean_text from main)
    from main import clean_text

    df[text_column] = df[text_column].apply(lambda t: clean_text(t, config))

    if config.get("remove_nulls", True):
        df = df[(df[text_column].str.strip() != "") & df[label_column].notna()]

    if config.get("remove_duplicates", True):
        df = df.drop_duplicates(subset=[text_column])

    if config.get("shuffle", True):
        seed = int(config.get("random_seed", 42))
        df = df.sample(frac=1, random_state=seed).reset_index(drop=True)

    # Store preprocessed data back in Redis with a new ID
    preprocessed_id = uuid.uuid4().hex
    redis_client.setex(
        f"preprocessed:{preprocessed_id}",
        3600 * 24 * 7,  # 7 days TTL
        cloudpickle.dumps(
            {
                "df": df,
                "text_column": text_column,
                "label_column": label_column,
                "config": config,
            }
        ),
    )
    return {"preprocessed_id": preprocessed_id}


@registry.register("train_model")
def train_model(payload: dict) -> dict:
    """
    Train a single model using parameters passed directly.
    """
    # All parameters come directly from the payload
    model_name = payload["model_name"]
    params = payload.get("params", {})
    preprocessed_id = payload["preprocessed_id"]
    test_size = payload.get("test_size", 0.25)
    random_state = payload.get("random_state", 42)
    max_features = payload.get("max_features", 5000)

    # Load preprocessed data
    preprocessed_data = redis_client.get(f"preprocessed:{preprocessed_id}")
    if preprocessed_data is None:
        raise ValueError(f"Preprocessed data {preprocessed_id} not found")
    entry = cloudpickle.loads(preprocessed_data)
    df = entry["df"]
    text_column = entry["text_column"]
    label_column = entry["label_column"]

    X_text = df[text_column]
    y = df[label_column]

    vectorizer = TfidfVectorizer(max_features=max_features)
    X = vectorizer.fit_transform(X_text)

    class_counts = y.value_counts()
    can_stratify = (class_counts >= 2).all() and len(class_counts) > 1
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y if can_stratify else None,
    )

    model = build_model(model_name, params)
    model.fit(X_train, y_train)

    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    train_acc = accuracy_score(y_train, train_pred)
    test_acc = accuracy_score(y_test, test_pred)
    report = classification_report(y_test, test_pred, zero_division=0, output_dict=True)
    labels = sorted(y.unique().tolist(), key=str)
    cm = confusion_matrix(y_test, test_pred, labels=labels).tolist()

    # Store the trained model in Redis for later download
    model_id = uuid.uuid4().hex
    redis_client.setex(
        f"model:{model_id}",
        3600 * 24 * 7,
        cloudpickle.dumps(
            {
                "model": model,
                "vectorizer": vectorizer,
                "model_name": model_name,
                "params": params,
                "preprocess_config": entry.get("config", {}),
                "text_column": text_column,
                "label_column": label_column,
                "train_acc": train_acc,
                "test_acc": test_acc,
                "report": report,
                "confusion_matrix": cm,
                "labels": labels,
            }
        ),
    )

    return {
        "model_id": model_id,
        "model_name": model_name,
        "params": params,
        "train_acc": float(train_acc),
        "test_acc": float(test_acc),
        "report": report,
        "confusion_matrix": cm,
        "labels": labels,
    }


@registry.register("evaluate_and_store")
def evaluate_and_store(payload: dict) -> dict:
    """Collect all fan-out results, select the best model, and finish the run."""
    run_id = payload["run_id"]
    final_task_id = payload["final_task_id"]
    training_task_ids = payload.get("training_task_ids") or []

    if not training_task_ids:
        raise ValueError("No training task IDs were supplied.")

    def unpack_task_result(value):
        if value is None:
            return None, None, None
        if isinstance(value, dict):
            return (
                value.get("status"),
                value.get("result", value.get("data", value.get("value"))),
                value.get("error") or value.get("message"),
            )
        return (
            getattr(value, "status", None),
            getattr(value, "result", None),
            getattr(value, "error", None),
        )

    candidates = []
    for task_id in training_task_ids:
        raw_result = result_backend.get_task_result(task_id)
        status, result_data, error = unpack_task_result(raw_result)
        normalized = str(status or "").lower()

        if normalized in {"failed", "failure", "cancelled", "canceled"}:
            raise RuntimeError(
                f"Training task {task_id} failed: {error or 'unknown error'}"
            )
        if normalized not in {"completed", "success"}:
            raise RuntimeError(
                f"Training task {task_id} is not complete. Current status: {status}"
            )
        if not isinstance(result_data, dict):
            raise RuntimeError(f"Training task {task_id} completed without a result.")

        model_id = result_data.get("model_id")
        if not model_id:
            raise RuntimeError(f"Training task {task_id} returned no model_id.")

        stored = redis_client.get(f"model:{model_id}")
        if stored is None:
            raise RuntimeError(f"Model {model_id} for task {task_id} was not found.")

        info = cloudpickle.loads(stored)
        candidates.append(
            {
                "task_id": task_id,
                "model_id": model_id,
                "model_name": info["model_name"],
                "params": info.get("params", {}),
                "train_acc": float(info["train_acc"]),
                "test_acc": float(info["test_acc"]),
                "report": info["report"],
                "confusion_matrix": info["confusion_matrix"],
                "labels": [str(label) for label in info["labels"]],
            }
        )

    best = max(candidates, key=lambda item: item["test_acc"])
    public_result = {
        **best,
        "run_id": run_id,
        "final_task_id": final_task_id,
        "candidate_count": len(candidates),
        "all_models": candidates,
    }

    redis_client.setex(
        f"run_result:{run_id}",
        3600 * 24 * 7,
        cloudpickle.dumps(public_result),
    )

    manifest_key = f"run:{run_id}"
    raw_manifest = redis_client.get(manifest_key)
    if raw_manifest:
        manifest = cloudpickle.loads(raw_manifest)
        manifest["status"] = "completed"
        manifest["model_id"] = best["model_id"]
        redis_client.setex(manifest_key, 3600 * 24 * 7, cloudpickle.dumps(manifest))

    return public_result
