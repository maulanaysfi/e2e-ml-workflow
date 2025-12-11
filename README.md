## Kubeflow End-to-End Retail Forecasting

End-to-end machine learning workflow that ingests online retail transactions, builds features, trains a LightGBM forecaster, and packages the flow as a Kubeflow Pipeline. Includes supporting Docker images, Kubernetes manifests, and notebooks for exploration.

### Repository Layout
- `pipelines/`: Python scripts for each step plus `kfp_compiler.py` to build `kubeflow_pipeline.yaml`.
- `datasets/`: DVC-tracked raw and processed samples used in the pipeline.
- `dockerimage/`: Docker build contexts for the KFP base image (`python-kfp`) and an API server used for ingestion.
- `k8s-deployments/`: Helm values and manifests for Airflow, MinIO/S3, PostgreSQL, ClickHouse, Kubeflow components, KServe, and supporting services.
- `k8s-dependencies/`: DNS (bind9), MetalLB IP pools, and related cluster plumbing.
- `notebooks/`: Exploratory notebooks for the online retail dataset and model experimentation.
- `dags/`: Airflow DAG example.
- `bin/`: Helper scripts for cluster bootstrap (OpenStack/GCP, RKE2, kubectl, etc.).

### Pipeline Stages
1) **Ingest** (`1_ingest_data.py` / `ingest_data` component): Pulls JSON from the data API, flattens records, writes CSV locally, uploads to `datalake/tmp`, and emits a dataset artifact.
2) **Merge** (`2_merge_data.py` / `merge_data` component): Downloads the newest tmp CSV from S3/MinIO, merges with the streaming file (`stream/online-retail-stream.csv`), and outputs a consolidated dataset.
3) **Preprocess & Feature Engineering** (`3_preprocess_data.py` / `preprocess_data` component): Type fixes, datetime-derived features, sales calculations, outlier handling, and writes the feature stream to `feature/online-retail-feature-stream.csv`.
4) **Explore** (`4_explore_data.py` / `explore_data` component): Generates descriptive plots (sales by year, daily trends) and saves artifacts.
5) **Train** (`5_train_model.py` / `train_model` component): Trains a LightGBM forecaster (via `ForecasterDirect`), evaluates MAE/RMSE, exports performance plots, and stores the model under `models/`.
6) **Serve** (`6_serve_model.py` / `serve_model` component): Retrieves the latest model artifact from S3/MinIO for downstream serving or testing.

### Quickstart
1) **Prereqs**: Python 3.10+, Kubeflow Pipelines v2 SDK, access to an S3-compatible bucket (MinIO or cloud), Docker (for images), and optionally a K8s cluster with Kubeflow.
2) **Auth**: Export S3 credentials for every step (local run and compiled pipeline):
   - `export S3_ACCESS_KEY_ID=...`
   - `export S3_SECRET_ACCESS_KEY=...`
   - `export S3_ENDPOINT_URL=https://<minio-or-s3-endpoint>`
3) **Install deps** (example):
   - `python -m venv .venv && source .venv/bin/activate`
   - `pip install -r dockerimage/python-kfp/requirements.txt`
4) **Compile the pipeline**:
   - `python pipelines/kfp_compiler.py`
   - Output: `pipelines/kubeflow_pipeline.yaml`
5) **Upload/Run**: Use the Kubeflow UI or `kfp` CLI to upload `kubeflow_pipeline.yaml`, set S3 secrets, and launch the run.

### Supporting Artifacts
- **Docker**: `dockerimage/python-kfp` defines the base image used by all KFP components; `dockerimage/api-server` contains a simple FastAPI data API plus sample dataset.
- **K8s Manifests**: `k8s-deployments/` covers Kubeflow apps, KServe, Airflow, MinIO, ClickHouse, PostgreSQL, etc.; `k8s-dependencies/` holds MetalLB and DNS config.
- **Data**: Sample raw/processed data and DVC pointers live under `datasets/`. Update `S3_*` env vars to point to your storage before running.
- **Notebooks**: Use the notebooks for interactive EDA or model tweaks; align changes with pipeline components for reproducibility.

### Notes
- The pipeline disables caching to keep each run fresh; enable caching in `kfp_compiler.py` if desired.
- All components expect S3/MinIO credentials via environment variables; missing values abort compilation and runs.
- Training outputs both a `.pkl` model and a PNG performance report for quick inspection.
