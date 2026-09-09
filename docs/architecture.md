# Architecture Diagrams

Two diagrams, per the report requirement: (1) the layered Lambda view, and (2) the
concrete component/deployment view (containers, topics, tables, ports).

Both are Mermaid, which renders natively on GitHub, in VS Code's Markdown preview, and
in most Markdown-aware tools. For the submitted PDF report, render this file (GitHub
preview, the VS Code Mermaid extension, or paste the source into
[mermaid.live](https://mermaid.live)) and paste the resulting image in — don't screenshot
this raw file.

## 1. Layered Lambda view

```mermaid
flowchart TB
    subgraph sources["Data Sources"]
        VP["Bedside monitors<br/>(vitals_producer.py)<br/>every 2s per patient"]
        LP["Pathology lab<br/>(lab_batch_producer.py)<br/>once per sim day"]
    end

    subgraph speed["SPEED LAYER — near-real-time"]
        direction TB
        K1["Kafka: vitals.raw<br/>(3 partitions, keyed by patient_id)"]
        SJ["stream_job.py<br/>clean → dedup (watermark) →<br/>1min/30s windows → NEWS2 score →<br/>sustained-alert detection"]
        ST1[("speed.vitals_clean<br/>speed.patient_vitals_1m<br/>speed.patient_risk_now<br/>speed.clinical_alerts")]
        K1 --> SJ --> ST1
    end

    subgraph batch["BATCH LAYER — authoritative daily recompute"]
        direction TB
        AR["raw_archiver.py<br/>(separate Structured Streaming job)"]
        MD[("MASTER DATASET<br/>Parquet, partitioned dt=/hour=<br/>data/master/vitals/")]
        LF["data/landing/labs/<br/>labs_&#123;sim_day&#125;.csv"]
        AF["Airflow DAG: daily_patient_risk<br/>wait → validate → batch_job.py"]
        BJ["batch_job.py<br/>worst-of-day NEWS2 (max_by) →<br/>5 lab DQ rules + quarantine →<br/>THE JOIN: adjust_risk_with_labs()"]
        BT[("batch.patient_daily_vitals<br/>batch.patient_lab_results<br/>batch.patient_risk_daily")]
        K1 --> AR --> MD
        MD --> BJ
        LF --> AF --> BJ
        BJ --> BT
    end

    subgraph serving["SERVING LAYER"]
        direction TB
        MV[("serving.patient_360<br/>materialized view<br/>(merges speed.* NOW with batch.* YESTERDAY)")]
        API["FastAPI (serving/api.py)"]
        GRAF["Grafana dashboards"]
        REP["Daily HTML/CSV report<br/>(report_builder.py)"]
        ST1 -.-> MV
        BT -.-> MV
        MV --> API
        MV --> GRAF
        BT --> REP
    end

    VP --> K1
    LP --> LF

    CLINICAL["common/clinical.py<br/>SHARED scoring logic<br/>(imported by BOTH SJ and BJ —<br/>one implementation, two execution contexts)"]
    CLINICAL -.-> SJ
    CLINICAL -.-> BJ

    style CLINICAL fill:#fff4d6,stroke:#c9971e
    style MV fill:#e6f6e6,stroke:#2e7d32
```

## 2. Concrete component / deployment view

```mermaid
flowchart LR
    subgraph ingestion["Ingestion containers"]
        VP2["vitals-producer<br/>:8001 metrics"]
        LP2["lab-producer<br/>:8002 metrics"]
    end

    subgraph kafka_box["kafka (KRaft, :29092 external)"]
        T1["vitals.raw (3p)"]
        T2["vitals.clean (3p)"]
        T3["clinical.alerts (1p)"]
        T4["vitals.dlq (1p)"]
    end

    subgraph spark_box["Spark containers"]
        SA["spark-archiver"]
        SS["spark-stream<br/>:4040 UI/metrics"]
    end

    subgraph airflow_box["Airflow containers (:8081 UI)"]
        AI["airflow-init"]
        AW["airflow-webserver"]
        ASch["airflow-scheduler<br/>runs batch_job.py via spark-submit"]
    end

    subgraph pg["postgres :5432"]
        DBV[("vitals db:<br/>speed / batch / serving / ops / reference schemas")]
        DBA[("airflow db")]
    end

    API2["api :8000"]
    GRAF2["grafana :3000"]
    PROM["prometheus :9090"]
    AM["alertmanager :9093"]
    KUI["kafka-ui :8080"]

    VP2 -->|produce| T1
    LP2 -->|writes| FS["data/landing/labs/ (bind mount)"]
    T1 --> SA --> MDF["data/master/vitals/ (bind mount)"]
    T1 --> SS
    SS --> T2
    SS --> T3
    SS -.dlq.-> T4
    SS --> DBV
    MDF --> ASch
    FS --> ASch
    ASch --> DBV
    ASch --> REPF["data/reports/ (bind mount)"]

    API2 --> DBV
    API2 -->|/metrics| PROM
    VP2 -->|/metrics| PROM
    LP2 -->|/metrics| PROM
    SS -->|/metrics| PROM
    PROM -->|alerts| AM
    AM -->|webhook| API2
    GRAF2 --> DBV
    GRAF2 --> PROM
    KUI --> kafka_box
```
